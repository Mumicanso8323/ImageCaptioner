using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Processing;

internal static class Program {
    // ======================
    // User-tunable defaults
    // ======================
    private const string DefaultAutoPrefixTags = "";
    private const string DefaultMinimalSubjectTags = "";

    // 画像サイズ制限（OpenAI送信対策）
    private const int MaxSideResize = 2048;

    private static readonly string[] ImageExts = [".png", ".jpg", ".jpeg", ".webp"];

    // ======================
    // LoRA caption shaping
    // ======================

    // 最優先タグ（LoRAで“寄り”が強くなるので先頭固定）
    private static string[] HardPrefixTags = [];

    // “重要な特徴量”っぽいタグ（これらは前方へ寄せる）
    // Danbooru tagger系を想定して、underscore前提
    private static readonly Regex[] PriorityRegex =
    [
        // 人数/主体
        new(@"^(1girl|1boy|solo|multiple_girls|multiple_boys|2girls|2boys)$", RegexOptions.IgnoreCase),

        // 髪・目（キャラ特徴）
        new(@"(hair|eyes|eyebrows|eyelashes|pupils)", RegexOptions.IgnoreCase),

        // 表情・感情
        new(@"(smile|frown|crying|angry|blush|serious|surprised|open_mouth|closed_mouth|tongue|tears)", RegexOptions.IgnoreCase),

        // 視線・顔向き
        new(@"(looking_at_viewer|looking_away|looking_back|head_tilt|profile|from_side|from_behind)", RegexOptions.IgnoreCase),

        // ポーズ・所作
        new(@"(pose|standing|sitting|lying|kneeling|crouching|walking|running|jumping|leaning|arms|hands|fingers|holding)", RegexOptions.IgnoreCase),

        // 衣装
        new(@"(dress|skirt|shirt|jacket|coat|hoodie|sweater|uniform|kimono|swimsuit|bikini|lingerie|socks|stockings|pantyhose|shoes|boots|gloves|hat|ribbon|bow|tie)", RegexOptions.IgnoreCase),

        // 背景/場所
        new(@"(indoors|outdoors|room|bedroom|street|city|school|classroom|park|forest|beach|sky|night|sunset|window|bed|chair)", RegexOptions.IgnoreCase),

        // 光・色・画作り
        new(@"(lighting|backlighting|rim_light|soft_light|hard_light|shadow|bloom|depth_of_field|bokeh|color|monochrome|warm|cool)", RegexOptions.IgnoreCase),

        // 画質系
        new(@"(highres|absurdres|masterpiece|best_quality|high_quality|detailed)", RegexOptions.IgnoreCase),
    ];

    // “あったら良いが優先度は低い”群（後ろに回す）
    private static readonly Regex[] LowPriorityRegex =
    [
        new(@"^(rating[:_].+)$", RegexOptions.IgnoreCase),
        new(@"(watermark|signature|artist_name|text|logo)", RegexOptions.IgnoreCase),
        new(@"(jpeg_artifacts|scan|lowres|worst_quality|low_quality)", RegexOptions.IgnoreCase),
    ];

    // 学習的に邪魔になりがちなもの（消す）
    private static readonly string[] DropTagsContains =
    [
        "negative:", "positive:", "sorry", "i can't", "i cannot", "cannot provide", "unable to",
        "as an ai", "i'm not able", "i won’t", "i won't"
    ];

    // ======================
    // Entry
    // ======================
    public static async Task<int> Main(string[] args) {
        if (args.Contains("--help") || args.Contains("-h")) {
            PrintHelp();
            return 0;
        }

        var launchWizard = args.Length == 0 || args.Contains("--wizard") || (args.Length > 0 && args[0].StartsWith('-', StringComparison.Ordinal));
        var options = launchWizard
            ? RunWizard()
            : ParseOptions(args);

        if (options is null)
            return 2;

        HardPrefixTags = SplitTags(options.AutoPrefixTags).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var minimalFallback = string.Join(", ", HardPrefixTags.Concat(SplitTags(options.MinimalSubjectTags)).Distinct(StringComparer.OrdinalIgnoreCase));

        if (!options.Quiet) {
            Console.WriteLine($"Dir={options.Dir}");
            Console.WriteLine($"Recursive={options.Recursive} Overwrite={options.Overwrite} KeepNeedtag={options.KeepNeedtag}");
            Console.WriteLine($"Concurrency={options.Concurrency} TargetBytes={options.TargetBytes} MinTags={options.MinTags} MaxTags={options.MaxTags}");
            Console.WriteLine($"JoyUrl={options.JoyUrl} JoyRetries={options.JoyRetries} JoyThreshold={options.JoyThresholdStart:0.00} Step={options.JoyThresholdStep:0.00}");
            Console.WriteLine($"OpenAI={(string.IsNullOrWhiteSpace(options.ApiKey) ? "OFF" : "ON")} Model={options.OpenAiModel} Retries={options.OpenAiRetries}");
            Console.WriteLine("Policy: Always write .txt even if tags are few.");
        }

        Process? autoStartedJoyTagProcess = null;
        if (options.AutoStartJoyTag) {
            var start = await StartOrReuseJoyTagServerAsync(options);
            if (start.ExitCode != 0)
                return start.ExitCode;

            autoStartedJoyTagProcess = start.AutoStartedProcess;
            if (autoStartedJoyTagProcess is not null)
                RegisterJoyTagShutdownHandlers(autoStartedJoyTagProcess);
        }

        using var httpJoy = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
        using var httpOpenAI = new HttpClient { Timeout = TimeSpan.FromSeconds(180) };
        if (!string.IsNullOrWhiteSpace(options.ApiKey))
            httpOpenAI.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", options.ApiKey);

        var opt = options.Recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
        var images = Directory.EnumerateFiles(options.Dir, "*.*", opt)
            .Where(p => ImageExts.Contains(Path.GetExtension(p), StringComparer.OrdinalIgnoreCase))
            .OrderBy(p => p, StringComparer.OrdinalIgnoreCase)
            .ToList();

        int ok = 0, skipped = 0, repaired = 0, low = 0, failed = 0;
        var errors = new ConcurrentBag<string>();

        var po = new ParallelOptions { MaxDegreeOfParallelism = options.Concurrency };

        await Parallel.ForEachAsync(images, po, async (imagePath, ct) => {
            var txtPath = Path.ChangeExtension(imagePath, ".txt");
            var needPath = Path.ChangeExtension(imagePath, ".needtag");

            try {
                // 既存txtの扱い
                if (!options.Overwrite && File.Exists(txtPath)) {
                    if (!options.ReprocessBadExisting) {
                        Interlocked.Increment(ref skipped);
                        return;
                    }

                    var existing = await File.ReadAllTextAsync(txtPath, ct);
                    var existingNorm = NormalizeAndShape(existing, options.MaxTags);

                    if (!LooksBad(existingNorm)) {
                        Interlocked.Increment(ref skipped);
                        return;
                    }

                    // 壊れてる既存txtは作り直す
                    TryDelete(txtPath);
                    Interlocked.Increment(ref repaired);
                }

                // 画像ロード＋必要なら3MBへ圧縮（OpenAI/HTTP送信用）
                var (mime, bytes) = await LoadAndMaybeCompressAsync(imagePath, options.TargetBytes, ct);

                // 1) JoyTag server（基本これが主役）
                string tagsJoy = await JoyTagAutoRetryAsync(
                    httpJoy, options.JoyUrl, bytes,
                    options.JoyThresholdStart, options.JoyThresholdStep,
                    options.JoyRetries, options.MinTags, ct);

                tagsJoy = NormalizeAndShape(tagsJoy, options.MaxTags);

                // 2) OpenAI（任意の補強。JoyTagが弱い/少ないときだけ）
                string tagsOpen = "";
                if (!string.IsNullOrWhiteSpace(options.ApiKey)) {
                    if (CountTags(tagsJoy) < options.MinTags || LooksBad(tagsJoy)) {
                        tagsOpen = await OpenAiRetryAsync(httpOpenAI, options.OpenAiModel, mime, bytes, options.OpenAiRetries, ct);
                        tagsOpen = NormalizeAndShape(tagsOpen, options.MaxTags);
                    }
                }

                // 3) マージ（“キャラ特徴”を落とさない方針）
                // - JoyTagを土台
                // - OpenAIが有益なら補完（ただしノイズもあるので強めに整形）
                string merged = MergePreferJoyThenOpen(tagsJoy, tagsOpen, options.MaxTags);

                // 4) 最終：style prefix + hard prefix tags
                merged = EnsureHardPrefixes(merged);

                // 5) txt欠損ゼロ：空/極端に短いなら最低限を書く
                if (string.IsNullOrWhiteSpace(merged) || CountTags(merged) < 3)
                    merged = EnsureHardPrefixes(minimalFallback);

                // 6) 書き込み
                await File.WriteAllTextAsync(txtPath, merged + Environment.NewLine, new UTF8Encoding(false), ct);

                // 7) needtag（低品質フラグ）
                bool isLow = CountTags(merged) < options.MinTags || LooksBad(merged);
                if (isLow) {
                    Interlocked.Increment(ref low);
                    if (options.KeepNeedtag)
                        await File.WriteAllTextAsync(needPath, "low tags; check later" + Environment.NewLine, new UTF8Encoding(false), ct);
                    else
                        TryDelete(needPath);
                } else {
                    TryDelete(needPath);
                }

                Interlocked.Increment(ref ok);

                if (!options.Quiet)
                    Console.WriteLine($"[ok] {Path.GetFileName(imagePath)} tags={CountTags(merged)}");
            } catch (Exception ex) {
                Interlocked.Increment(ref failed);
                errors.Add($"[fail] {Path.GetFileName(imagePath)}: {ex.Message}");

                // 最悪でもtxtだけは作る（要望）
                try {
                    if (!File.Exists(txtPath))
                        await File.WriteAllTextAsync(txtPath, EnsureHardPrefixes(minimalFallback) + Environment.NewLine, new UTF8Encoding(false), ct);

                    if (options.KeepNeedtag)
                        await File.WriteAllTextAsync(needPath, "error; check later" + Environment.NewLine, new UTF8Encoding(false), ct);
                } catch { /* ignore */ }
            }
        });

        Console.WriteLine($"Done. ok={ok}, repaired={repaired}, skipped={skipped}, low={low}, failed={failed}");
        if (!errors.IsEmpty) {
            Console.WriteLine("---- Errors (summary) ----");
            foreach (var e in errors.Take(20)) Console.WriteLine(e);
            if (errors.Count > 20) Console.WriteLine($"(and {errors.Count - 20} more)");
        }

        return failed == 0 ? 0 : 1;
    }

    private static AppOptions? ParseOptions(string[] args) {
        var dir = args[0];
        if (!Directory.Exists(dir)) {
            Console.Error.WriteLine($"Directory not found: {dir}");
            return null;
        }

        var options = new AppOptions {
            Dir = dir,
            Recursive = args.Contains("--recursive"),
            Overwrite = args.Contains("--overwrite"),
            Quiet = args.Contains("--quiet"),
            KeepNeedtag = args.Contains("--keep-needtag"),
            Concurrency = Clamp(ParseInt(GetArg(args, "--concurrency"), 8), 1, 64),
            TargetBytes = Clamp(ParseInt(GetArg(args, "--target-bytes"), 3_000_000), 400_000, 20_000_000),
            MinTags = Clamp(ParseInt(GetArg(args, "--min-tags"), 30), 5, 200),
            MaxTags = Clamp(ParseInt(GetArg(args, "--max-tags"), 80), 20, 300),
            JoyUrl = GetArg(args, "--joy-url") ?? "http://127.0.0.1:7865",
            JoyRetries = Clamp(ParseInt(GetArg(args, "--joy-retries"), 3), 0, 10),
            JoyThresholdStart = ClampDouble(ParseDouble(GetArg(args, "--joy-threshold"), 0.40), 0.10, 0.90),
            JoyThresholdStep = ClampDouble(ParseDouble(GetArg(args, "--joy-threshold-step"), 0.05), 0.01, 0.20),
            OpenAiModel = GetArg(args, "--model") ?? "gpt-4.1-mini",
            OpenAiRetries = Clamp(ParseInt(GetArg(args, "--openai-retries"), 1), 0, 6),
            ReprocessBadExisting = !args.Contains("--no-reprocess-bad-existing"),
            AutoPrefixTags = GetArg(args, "--auto-prefix-tags") ?? "",
            MinimalSubjectTags = GetArg(args, "--minimal-subject-tags") ?? "",
            AutoStartJoyTag = args.Contains("--auto-start-joytag"),
            JoyTagPythonExe = GetArg(args, "--joytag-python"),
            JoyTagWorkingDir = GetArg(args, "--joytag-dir")
        };

        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        if (args.Contains("--no-openai")) apiKey = null;
        options.ApiKey = apiKey;

        if (!HasUserDefinedMinimumTags(options))
            return null;

        return options;
    }

    private static AppOptions? RunWizard() {
        var saved = LoadWizardConfig();
        Console.WriteLine("=== ImageCaptioner Wizard ===");
        Console.WriteLine("Enterキーだけで前回値を再利用します。\n");

        var dir = PromptString("画像フォルダ", saved.LastDir);
        if (string.IsNullOrWhiteSpace(dir) || !Directory.Exists(dir)) {
            Console.Error.WriteLine("有効なフォルダを指定してください。");
            return null;
        }

        var joyUrl = PromptString("JoyTag URL", saved.JoyUrl);
        var autoPrefixTags = PromptString("常に先頭へ付けるタグ(カンマ区切り)", saved.AutoPrefixTags);
        var minimalSubjectTags = PromptString("最低限タグ(失敗時フォールバック)", saved.MinimalSubjectTags);

        var options = new AppOptions {
            Dir = dir,
            Recursive = PromptBoolByEmpty("再帰処理する？(Enter=Yes, 文字入力でNo)", saved.Recursive),
            Overwrite = PromptBoolByEmpty("既存txtも上書きする？(Enter=Yes, 文字入力でNo)", saved.Overwrite),
            Quiet = PromptBoolByEmpty("ログ少なめにする？(Enter=Yes, 文字入力でNo)", saved.Quiet),
            KeepNeedtag = PromptBoolByEmpty(".needtagを残す？(Enter=Yes, 文字入力でNo)", saved.KeepNeedtag),
            ReprocessBadExisting = true,
            Concurrency = PromptInt("並列数", saved.Concurrency, 1, 64),
            MinTags = PromptInt("最小タグ数", saved.MinTags, 5, 200),
            MaxTags = PromptInt("最大タグ数", saved.MaxTags, 20, 300),
            TargetBytes = PromptInt("送信用ターゲットサイズ(bytes)", saved.TargetBytes, 400_000, 20_000_000),
            JoyUrl = string.IsNullOrWhiteSpace(joyUrl) ? "http://127.0.0.1:7865" : joyUrl,
            JoyRetries = PromptInt("JoyTagリトライ回数", saved.JoyRetries, 0, 10),
            JoyThresholdStart = PromptDouble("JoyTag threshold開始値", saved.JoyThresholdStart, 0.10, 0.90),
            JoyThresholdStep = PromptDouble("JoyTag threshold減少幅", saved.JoyThresholdStep, 0.01, 0.20),
            OpenAiModel = saved.OpenAiModel,
            OpenAiRetries = saved.OpenAiRetries,
            ApiKey = null,
            AutoPrefixTags = autoPrefixTags.Trim(),
            MinimalSubjectTags = minimalSubjectTags.Trim(),
            AutoStartJoyTag = PromptBoolByEmpty("JoyTagサーバーを自動起動する？(Enter=Yes, 文字入力でNo)", saved.AutoStartJoyTag),
            JoyTagWorkingDir = PromptString("JoyTagフォルダ", saved.JoyTagWorkingDir),
            JoyTagPythonExe = PromptString("JoyTag python.exe", saved.JoyTagPythonExe)
        };

        var openAiOff = PromptBoolByEmpty("OpenAIを無効化する？(Enter=Yes, 文字入力でNo)", true);
        if (!openAiOff) {
            options.ApiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
            options.OpenAiModel = PromptString("OpenAIモデル", saved.OpenAiModel);
            options.OpenAiRetries = PromptInt("OpenAIリトライ", saved.OpenAiRetries, 0, 6);
        }

        if (!HasUserDefinedMinimumTags(options)) {
            Console.Error.WriteLine("最低限タグはユーザー入力で指定してください（auto-prefix-tags または minimal-subject-tags）。");
            return null;
        }

        SaveWizardConfig(new WizardConfig {
            LastDir = options.Dir,
            JoyUrl = options.JoyUrl,
            Concurrency = options.Concurrency,
            MinTags = options.MinTags,
            MaxTags = options.MaxTags,
            TargetBytes = options.TargetBytes,
            Recursive = options.Recursive,
            Overwrite = options.Overwrite,
            Quiet = options.Quiet,
            KeepNeedtag = options.KeepNeedtag,
            AutoPrefixTags = options.AutoPrefixTags,
            MinimalSubjectTags = options.MinimalSubjectTags,
            AutoStartJoyTag = options.AutoStartJoyTag,
            JoyTagWorkingDir = options.JoyTagWorkingDir,
            JoyTagPythonExe = options.JoyTagPythonExe,
            JoyRetries = options.JoyRetries,
            JoyThresholdStart = options.JoyThresholdStart,
            JoyThresholdStep = options.JoyThresholdStep,
            OpenAiModel = options.OpenAiModel,
            OpenAiRetries = options.OpenAiRetries
        });

        return options;
    }

    private static bool HasUserDefinedMinimumTags(AppOptions options) {
        var hasAny = SplitTags(options.AutoPrefixTags).Any() || SplitTags(options.MinimalSubjectTags).Any();
        if (hasAny) return true;

        Console.Error.WriteLine("最低限タグが未設定です。--auto-prefix-tags または --minimal-subject-tags で指定してください。");
        return false;
    }

    private static async Task<(int ExitCode, Process? AutoStartedProcess)> StartOrReuseJoyTagServerAsync(AppOptions options) {
        if (await IsJoyTagAliveAsync(options.JoyUrl, CancellationToken.None)) {
            Console.WriteLine("JoyTag is already alive. Reusing existing server.");
            return (0, null);
        }

        if (!TryGetPortOwnerPids(options.JoyUrl, out var pids)) {
            Console.Error.WriteLine("[error] Failed to inspect port owner process on JoyTag port.");
            return (2, null);
        }

        if (pids.Count > 0) {
            foreach (var pid in pids) {
                if (!TryKillProcessTree(pid)) {
                    Console.Error.WriteLine($"[error] Failed to kill PID {pid} on JoyTag port. Run as administrator.");
                    return (2, null);
                }
            }
        }

        if (string.IsNullOrWhiteSpace(options.JoyTagWorkingDir) || string.IsNullOrWhiteSpace(options.JoyTagPythonExe)) {
            Console.Error.WriteLine("[error] JoyTag auto-start requires joytag-dir and joytag-python.");
            return (2, null);
        }

        var psi = new ProcessStartInfo {
            FileName = options.JoyTagPythonExe,
            WorkingDirectory = options.JoyTagWorkingDir,
            Arguments = $"-m uvicorn joytag_server:app --host 127.0.0.1 --port {GetPort(options.JoyUrl)} --workers 1",
            UseShellExecute = false,
            CreateNoWindow = false
        };

        var p = Process.Start(psi);
        if (p is null) {
            Console.Error.WriteLine("[error] Failed to start JoyTag process.");
            return (2, null);
        }

        for (int i = 0; i < 30; i++) {
            if (await IsJoyTagAliveAsync(options.JoyUrl, CancellationToken.None)) {
                Console.WriteLine("JoyTag started and is ready.");
                return (0, p);
            }
            await Task.Delay(1000);
        }

        Console.Error.WriteLine("[error] JoyTag did not become ready within 30 seconds.");
        try { if (!p.HasExited) p.Kill(entireProcessTree: true); } catch { }
        return (3, null);
    }

    private static void RegisterJoyTagShutdownHandlers(Process process) {
        void KillIfRunning() {
            try {
                if (!process.HasExited)
                    process.Kill(entireProcessTree: true);
            } catch { }
        }

        Console.CancelKeyPress += (_, e) => {
            e.Cancel = false;
            KillIfRunning();
        };
        AppDomain.CurrentDomain.ProcessExit += (_, __) => KillIfRunning();
    }

    private static async Task<bool> IsJoyTagAliveAsync(string joyUrl, CancellationToken ct) {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };

        async Task<bool> CheckAsync(string suffix, string[] keywords) {
            var url = $"{joyUrl.TrimEnd('/')}{suffix}";
            try {
                using var resp = await http.GetAsync(url, ct);
                if (!resp.IsSuccessStatusCode) return false;
                var text = (await resp.Content.ReadAsStringAsync(ct)).ToLowerInvariant();
                return keywords.Any(k => text.Contains(k));
            } catch {
                return false;
            }
        }

        if (await CheckAsync("/openapi.json", ["openapi", "paths", "swagger"]))
            return true;

        if (await CheckAsync("/docs", ["swagger", "openapi", "fastapi"]))
            return true;

        return false;
    }

    private static bool TryGetPortOwnerPids(string joyUrl, out List<int> pids) {
        pids = new List<int>();
        try {
            if (!OperatingSystem.IsWindows()) return true;

            var port = GetPort(joyUrl);
            var psi = new ProcessStartInfo {
                FileName = "powershell",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                Arguments = "-NoProfile -Command \"$port=" + port + "; netstat -ano | findstr :$port | % { ($_ -split '\\s+')[-1] } | ? { $_ -match '^\\d+$' } | sort -Unique\""
            };

            using var p = Process.Start(psi);
            if (p is null) return false;
            var output = p.StandardOutput.ReadToEnd();
            p.WaitForExit(3000);

            foreach (var line in output.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries)) {
                if (int.TryParse(line.Trim(), out var pid))
                    pids.Add(pid);
            }
            return true;
        } catch {
            return false;
        }
    }

    private static bool TryKillProcessTree(int pid) {
        try {
            if (!OperatingSystem.IsWindows()) return true;

            var psi = new ProcessStartInfo {
                FileName = "taskkill",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                Arguments = $"/PID {pid} /F /T"
            };
            using var p = Process.Start(psi);
            if (p is null) return false;
            p.WaitForExit(5000);
            return p.ExitCode == 0;
        } catch {
            return false;
        }
    }

    // ======================
    // JoyTag client
    // ======================
    private static async Task<string> JoyTagAutoRetryAsync(
        HttpClient http, string baseUrl, byte[] imageBytes,
        double thresholdStart, double thresholdStep,
        int retries, int minTags, CancellationToken ct) {
        double thr = thresholdStart;
        string last = "";

        // 閾値を下げるほどタグが増える（ノイズも増える）
        for (int i = 0; i <= retries; i++) {
            last = await CallJoyTagAsync(http, baseUrl, imageBytes, thr, ct);
            var shaped = NormalizeAndShape(last, 999); // ここでは数制限せず評価

            if (!string.IsNullOrWhiteSpace(shaped) && CountTags(shaped) >= minTags)
                return shaped;

            thr = Math.Max(0.10, thr - thresholdStep);
        }

        // 「少なくても返す」：最後の結果を返す
        return NormalizeAndShape(last, 999);
    }

    private static async Task<string> CallJoyTagAsync(HttpClient http, string baseUrl, byte[] imageBytes, double threshold, CancellationToken ct) {
        var url = $"{baseUrl.TrimEnd('/')}/tag?threshold={threshold:0.00}";

        using var form = new MultipartFormDataContent();
        var file = new ByteArrayContent(imageBytes);
        file.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        form.Add(file, "file", "image.jpg");

        using var resp = await http.PostAsync(url, form, ct);
        var text = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode) return "";

        using var doc = JsonDocument.Parse(text);
        if (!doc.RootElement.TryGetProperty("tags", out var tagsEl) || tagsEl.ValueKind != JsonValueKind.Array)
            return "";

        var tags = new List<string>();
        foreach (var t in tagsEl.EnumerateArray()) {
            if (t.ValueKind == JsonValueKind.String) {
                var s = t.GetString();
                if (!string.IsNullOrWhiteSpace(s)) tags.Add(s.Trim());
            }
        }
        return string.Join(", ", tags);
    }

    // ======================
    // OpenAI optional
    // ======================
    private static async Task<string> OpenAiRetryAsync(HttpClient http, string model, string mime, byte[] imageBytes, int retries, CancellationToken ct) {
        // 文章化・拒否・POS/NEGを抑止しつつ特徴量を強制
        string promptA =
            "Return ONE line of comma-separated Danbooru-style tags for LoRA training.\n" +
            "ONLY tags. No sentences. No POSITIVE/NEGATIVE.\n" +
            "Must include: hair color, hair style, facial expression, pose, gaze/gesture, outfit.\n" +
            "Also include: style, lighting, background if visible.\n" +
            "Avoid explicit sexual acts. Keep description non-explicit.\n" +
            "Target 50-80 tags.";

        string promptB =
            "Output ONLY comma-separated tags (one line). No extra words.\n" +
            "Include: hair/eyes, expression, pose, outfit, background, lighting, quality/style.\n" +
            "Non-explicit.\n" +
            "Target 60+ tags.";

        string last = "";
        for (int i = 0; i <= retries; i++) {
            last = await CallResponsesApiOnceAsync(http, model, mime, imageBytes, i == 0 ? promptA : promptB, ct);
            if (!string.IsNullOrWhiteSpace(last)) return last;
        }
        return last;
    }

    private static async Task<string> CallResponsesApiOnceAsync(HttpClient http, string model, string mime, byte[] imageBytes, string userPrompt, CancellationToken ct) {
        var base64 = Convert.ToBase64String(imageBytes);
        var payload = new {
            model,
            input = new object[]
            {
                new
                {
                    role = "user",
                    content = new object[]
                    {
                        new { type = "input_text", text = userPrompt },
                        new { type = "input_image", image_url = $"data:{mime};base64,{base64}" }
                    }
                }
            },
            max_output_tokens = 400
        };

        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var resp = await http.PostAsync("https://api.openai.com/v1/responses", content, ct);
        var respText = await resp.Content.ReadAsStringAsync(ct);

        if (!resp.IsSuccessStatusCode)
            return "";

        using var doc = JsonDocument.Parse(respText);

        if (doc.RootElement.TryGetProperty("output_text", out var ot) && ot.ValueKind == JsonValueKind.String)
            return ot.GetString() ?? "";

        // fallback parse
        if (doc.RootElement.TryGetProperty("output", out var output) && output.ValueKind == JsonValueKind.Array) {
            foreach (var item in output.EnumerateArray()) {
                if (!item.TryGetProperty("content", out var contents) || contents.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var c in contents.EnumerateArray()) {
                    if (c.ValueKind != JsonValueKind.Object) continue;
                    if (c.TryGetProperty("type", out var t) && t.GetString() == "output_text" &&
                        c.TryGetProperty("text", out var te) && te.ValueKind == JsonValueKind.String)
                        return te.GetString() ?? "";
                }
            }
        }

        return "";
    }

    // ======================
    // Tag shaping for LoRA
    // ======================
    private static string MergePreferJoyThenOpen(string joy, string open, int maxTags) {
        var a = SplitTags(joy).ToList();
        var b = SplitTags(open).ToList();

        // Joy土台→Openで補完（重複除去）
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var merged = new List<string>();
        foreach (var t in a) {
            if (set.Add(t)) merged.Add(t);
        }
        foreach (var t in b) {
            if (set.Add(t)) merged.Add(t);
        }

        // 整形（優先順に並べ替え＋上限）
        var shaped = ShapeTagsForLora(merged, maxTags);
        return string.Join(", ", shaped);
    }

    private static string EnsureHardPrefixes(string line) {
        var tags = SplitTags(line).ToList();
        var set = new HashSet<string>(tags, StringComparer.OrdinalIgnoreCase);

        // HardPrefixTags を先頭に固定
        var result = new List<string>();
        foreach (var p in HardPrefixTags) {
            if (!set.Contains(p)) {
                result.Add(p);
                set.Add(p);
            } else {
                // 既にあるなら後で重複しないように一旦先頭側へ持ってくる
                result.Add(p);
            }
        }

        // 既存タグのうち prefix 以外を追加
        foreach (var t in tags) {
            if (!HardPrefixTags.Contains(t, StringComparer.OrdinalIgnoreCase) && set.Contains(t))
                result.Add(t);
        }

        // 再整形（順序が崩れないようにshape）
        return string.Join(", ", ShapeTagsForLora(result, Math.Max(30, result.Count)));
    }

    private static string NormalizeAndShape(string raw, int maxTags) {
        if (string.IsNullOrWhiteSpace(raw)) return "";

        var s = raw.Replace("\r\n", "\n").Trim();

        // POSITIVE/NEGATIVEが混じっても救う（POSITIVEのみ抜く）
        var pos = ExtractAfterPrefixLine(s, "POSITIVE:");
        if (!string.IsNullOrWhiteSpace(pos)) s = pos;

        // NEGATIVE以降は切る（学習キャプションに不要）
        s = CutAfterPrefix(s, "NEGATIVE:");

        // コードフェンス除去
        s = StripCodeFences(s);

        // 改行→空白
        s = s.Replace("\n", " ").Trim();

        // 明らかな拒否/ゴミは弾く（ただし“空にして最終フォールバック”に落ちる）
        if (LooksBad(s)) return "";

        var tags = SplitTags(s)
            .Select(NormalizeTag)
            .Where(t => t.Length >= 2)
            .Where(t => !ShouldDropTag(t))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var shaped = ShapeTagsForLora(tags, maxTags);
        return string.Join(", ", shaped);
    }

    private static List<string> ShapeTagsForLora(IEnumerable<string> tags, int maxTags) {
        // バケット分けして順序付け：
        // 1) HardPrefix（StyleTag等）
        // 2) PriorityRegex にヒットする“特徴量”
        // 3) その他
        // 4) LowPriorityRegex（最後に回す）
        var list = tags
            .Select(NormalizeTag)
            .Where(t => t.Length >= 2)
            .Where(t => !ShouldDropTag(t))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var set = new HashSet<string>(list, StringComparer.OrdinalIgnoreCase);

        var prefix = new List<string>();
        foreach (var p in HardPrefixTags)
            if (set.Contains(p)) prefix.Add(p);

        // prefixを除いて分類
        var rest = list.Where(t => !HardPrefixTags.Contains(t, StringComparer.OrdinalIgnoreCase)).ToList();

        var low = new List<string>();
        var pri = new List<string>();
        var mid = new List<string>();

        foreach (var t in rest) {
            if (IsLowPriority(t)) low.Add(t);
            else if (IsPriority(t)) pri.Add(t);
            else mid.Add(t);
        }

        // priの中も、より優先（人物・髪目・表情…）が先に来るように簡易スコア
        pri = pri
            .Select(t => (t, score: PriorityScore(t)))
            .OrderByDescending(x => x.score)
            .ThenBy(x => x.t, StringComparer.OrdinalIgnoreCase)
            .Select(x => x.t)
            .ToList();

        // mid/low はアルファベット順で安定化
        mid = mid.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
        low = low.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();

        var outList = new List<string>();
        outList.AddRange(prefix);
        outList.AddRange(pri);
        outList.AddRange(mid);
        outList.AddRange(low);

        // 先頭から maxTags まで
        if (outList.Count > maxTags)
            outList = outList.Take(maxTags).ToList();

        return outList;
    }

    private static int PriorityScore(string t) {
        int score = 0;
        for (int i = 0; i < PriorityRegex.Length; i++) {
            if (PriorityRegex[i].IsMatch(t))
                score += (PriorityRegex.Length - i) * 10;
        }
        return score;
    }

    private static bool IsPriority(string t) => PriorityRegex.Any(r => r.IsMatch(t));
    private static bool IsLowPriority(string t) => LowPriorityRegex.Any(r => r.IsMatch(t));

    private static bool ShouldDropTag(string t) {
        var lower = t.ToLowerInvariant();
        return DropTagsContains.Any(x => lower.Contains(x));
    }

    private static bool LooksBad(string s) {
        if (string.IsNullOrWhiteSpace(s)) return true;
        var lower = s.ToLowerInvariant();
        if (DropTagsContains.Any(x => lower.Contains(x))) return true;

        // カンマがほぼ無い＆文章っぽい → bad
        int commas = s.Count(c => c == ',');
        if (commas < 2 && s.Length > 40) return true;

        return false;
    }

    private static int CountTags(string line) => SplitTags(line).Count();

    private static IEnumerable<string> SplitTags(string s) {
        if (string.IsNullOrWhiteSpace(s)) yield break;
        foreach (var part in s.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            var t = part.Trim();
            if (!string.IsNullOrWhiteSpace(t)) yield return t;
        }
    }

    private static string NormalizeTag(string t) {
        t = t.Trim().Trim('"', '\'', '`');

        // 句点や末尾の不要文字を落とす
        t = Regex.Replace(t, @"[;。、\.]+$", "");

        // 空白を underscore に寄せる（danbooru系タグ互換）
        t = Regex.Replace(t, @"\s+", "_");

        // underscoreの連続を詰める
        t = Regex.Replace(t, @"_+", "_");

        return t;
    }

    private static string StripCodeFences(string s) {
        s = s.Trim();
        if (!s.StartsWith("```")) return s;

        var first = s.IndexOf('\n');
        if (first >= 0) s = s[(first + 1)..];

        var lastFence = s.LastIndexOf("```", StringComparison.Ordinal);
        if (lastFence >= 0) s = s[..lastFence];

        return s.Trim();
    }

    private static string ExtractAfterPrefixLine(string text, string prefix) {
        foreach (var line in text.Split('\n')) {
            var t = line.Trim();
            if (t.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return t[prefix.Length..].Trim();
        }
        return "";
    }

    private static string CutAfterPrefix(string text, string prefix) {
        var idx = text.IndexOf(prefix, StringComparison.OrdinalIgnoreCase);
        if (idx >= 0) return text[..idx].Trim();
        return text;
    }

    // ======================
    // Image load + compress
    // ======================
    private static async Task<(string mime, byte[] bytes)> LoadAndMaybeCompressAsync(string path, int targetBytes, CancellationToken ct) {
        var ext = Path.GetExtension(path).ToLowerInvariant();
        var orig = await File.ReadAllBytesAsync(path, ct);

        var origMime = ext switch {
            ".png" => "image/png",
            ".jpg" => "image/jpeg",
            ".jpeg" => "image/jpeg",
            ".webp" => "image/webp",
            _ => "application/octet-stream"
        };

        if (orig.Length <= targetBytes)
            return (origMime, orig);

        using var img = Image.Load(orig);

        // でかすぎる画像は縮小
        var maxSide = Math.Max(img.Width, img.Height);
        if (maxSide > MaxSideResize) {
            var scale = (double)MaxSideResize / maxSide;
            var nw = (int)(img.Width * scale);
            var nh = (int)(img.Height * scale);
            img.Mutate(x => x.Resize(nw, nh));
        }

        // JPEGにしてサイズを targetBytes 付近に収める（二分探索）
        int lo = 35, hi = 92;
        byte[] best = Array.Empty<byte>();

        while (lo <= hi) {
            int q = (lo + hi) / 2;
            using var ms = new MemoryStream();
            img.Save(ms, new JpegEncoder { Quality = q });
            var b = ms.ToArray();
            best = b;

            if (b.Length > targetBytes) hi = q - 1;
            else lo = q + 1;
        }

        return ("image/jpeg", best);
    }

    // ======================
    // CLI utils
    // ======================
    private static string? GetArg(string[] args, string name) {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i].Equals(name, StringComparison.OrdinalIgnoreCase))
                return args[i + 1];
        return null;
    }

    private static int ParseInt(string? s, int fallback) => int.TryParse(s, out var v) ? v : fallback;
    private static double ParseDouble(string? s, double fallback) => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : fallback;

    private static int Clamp(int v, int lo, int hi) => Math.Max(lo, Math.Min(hi, v));
    private static double ClampDouble(double v, double lo, double hi) => Math.Max(lo, Math.Min(hi, v));

    private static void TryDelete(string path) {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }


    private static string PromptString(string label, string fallback) {
        Console.Write($"{label} [{fallback}]: ");
        var input = Console.ReadLine();
        return string.IsNullOrWhiteSpace(input) ? fallback : input.Trim();
    }

    private static bool PromptBoolByEmpty(string label, bool fallback) {
        while (true) {
            Console.Write($"{label} (Enter=前回値, 現在値: {fallback}) [y/n]: ");
            var input = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(input))
                return fallback;

            var t = input.Trim().ToLowerInvariant();
            if (t is "1" or "true" or "t" or "yes" or "y") return true;
            if (t is "0" or "false" or "f" or "no" or "n") return false;

            Console.WriteLine("入力は y/n, yes/no, true/false, 1/0 のいずれかで指定してください。");
        }
    }

    private static int PromptInt(string label, int fallback, int min, int max) {
        var text = PromptString(label, fallback.ToString(CultureInfo.InvariantCulture));
        return Clamp(ParseInt(text, fallback), min, max);
    }

    private static double PromptDouble(string label, double fallback, double min, double max) {
        var text = PromptString(label, fallback.ToString("0.00", CultureInfo.InvariantCulture));
        return ClampDouble(ParseDouble(text, fallback), min, max);
    }

    private static string GetWizardConfigPath() {
        var baseDir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        var dir = Path.Combine(baseDir, "ImageCaptioner");
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "wizard-config.json");
    }

    private static WizardConfig LoadWizardConfig() {
        try {
            var path = GetWizardConfigPath();
            if (!File.Exists(path)) return new WizardConfig();
            var json = File.ReadAllText(path);
            var cfg = JsonSerializer.Deserialize<WizardConfig>(json);
            return cfg ?? new WizardConfig();
        } catch {
            return new WizardConfig();
        }
    }

    private static void SaveWizardConfig(WizardConfig config) {
        try {
            var path = GetWizardConfigPath();
            var json = JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(path, json, new UTF8Encoding(false));
        } catch {
        }
    }

    private static int GetPort(string joyUrl) {
        if (Uri.TryCreate(joyUrl, UriKind.Absolute, out var uri) && uri.Port > 0)
            return uri.Port;
        return 7865;
    }

    private sealed class AppOptions {
        public string Dir { get; set; } = "";
        public bool Recursive { get; set; } = true;
        public bool Overwrite { get; set; }
        public bool Quiet { get; set; } = true;
        public bool KeepNeedtag { get; set; } = true;
        public int Concurrency { get; set; } = 12;
        public int TargetBytes { get; set; } = 3_000_000;
        public int MinTags { get; set; } = 30;
        public int MaxTags { get; set; } = 80;
        public string JoyUrl { get; set; } = "http://127.0.0.1:7865";
        public int JoyRetries { get; set; } = 3;
        public double JoyThresholdStart { get; set; } = 0.4;
        public double JoyThresholdStep { get; set; } = 0.05;
        public string OpenAiModel { get; set; } = "gpt-4.1-mini";
        public int OpenAiRetries { get; set; } = 1;
        public string? ApiKey { get; set; }
        public bool ReprocessBadExisting { get; set; } = true;
        public string AutoPrefixTags { get; set; } = "";
        public string MinimalSubjectTags { get; set; } = "";
        public bool AutoStartJoyTag { get; set; }
        public string? JoyTagPythonExe { get; set; }
        public string? JoyTagWorkingDir { get; set; }
    }

    private sealed class WizardConfig {
        public string LastDir { get; set; } = "";
        public string JoyUrl { get; set; } = "http://127.0.0.1:7865";
        public int Concurrency { get; set; } = 12;
        public int MinTags { get; set; } = 30;
        public int MaxTags { get; set; } = 80;
        public int TargetBytes { get; set; } = 3_000_000;
        public bool Recursive { get; set; } = true;
        public bool Overwrite { get; set; }
        public bool Quiet { get; set; } = true;
        public bool KeepNeedtag { get; set; } = true;
        public string AutoPrefixTags { get; set; } = "";
        public string MinimalSubjectTags { get; set; } = "";
        public bool AutoStartJoyTag { get; set; } = true;
        public string JoyTagWorkingDir { get; set; } = @"D:\tools\joytag";
        public string JoyTagPythonExe { get; set; } = @"D:\tools\joytag\venv\Scripts\python.exe";
        public int JoyRetries { get; set; } = 3;
        public double JoyThresholdStart { get; set; } = 0.4;
        public double JoyThresholdStep { get; set; } = 0.05;
        public string OpenAiModel { get; set; } = "gpt-4.1-mini";
        public int OpenAiRetries { get; set; } = 1;
    }

    private static void PrintHelp() {
        Console.WriteLine(
@"ImageCaptioner (Final: JoyTag server + optional OpenAI, LoRA-friendly tags, always writes .txt)

Usage:
  ImageCaptioner                  # wizard mode (recommended for .exe)
  ImageCaptioner <dir> [--recursive] [--overwrite] [--quiet]
    [--concurrency N] [--target-bytes BYTES]
    [--min-tags N] [--max-tags N]
    [--joy-url http://127.0.0.1:7865] [--joy-retries N]
    [--joy-threshold 0.40] [--joy-threshold-step 0.05]
    [--model MODEL] [--openai-retries N]
    [--keep-needtag]
    [--no-reprocess-bad-existing]
    [--auto-prefix-tags <comma,separated,tags>]
    [--minimal-subject-tags <comma,separated,tags>]
    [--auto-start-joytag --joytag-dir D:\tools\joytag --joytag-python D:\tools\joytag\venv\Scripts\python.exe]

Notes:
  - ALWAYS writes .txt (even if tags are few).
  - Default: JoyTag server first; OpenAI only when JoyTag is weak (OPENAI_API_KEY required).
  - LoRA shaping: prefixes first, character features early, background/quality later, dedupe, drop junk.

Recommended:
  Wizard mode auto-saves previous values and can auto-start JoyTag:
    ImageCaptioner.exe

  CLI run:
    dotnet run -- ""D:\dataset"" --recursive --quiet --concurrency 12 --min-tags 30 --max-tags 80 --target-bytes 3000000 --keep-needtag --joy-url ""http://127.0.0.1:7865""
");
    }
}
