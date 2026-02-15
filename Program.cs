using System.Collections.Concurrent;
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
    private const string StyleTag = "mumistyle";

    // txt欠損が最悪なので、最終フォールバックは必ず書ける「安全な最低限」
    private const string MinimalFallback = "mumistyle, manga_style, lineart, 1girl, solo";

    // 画像サイズ制限（OpenAI送信対策）
    private const int MaxSideResize = 2048;

    private static readonly string[] ImageExts = [".png", ".jpg", ".jpeg", ".webp"];

    // ======================
    // LoRA caption shaping
    // ======================

    // 最優先タグ（LoRAで“寄り”が強くなるので先頭固定）
    private static readonly string[] HardPrefixTags =
    [
        StyleTag,
        "manga_style",
        "lineart"
    ];

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
        if (args.Length == 0 || args.Contains("--help") || args.Contains("-h")) {
            PrintHelp();
            return 0;
        }

        var dir = args[0];
        if (!Directory.Exists(dir)) {
            Console.Error.WriteLine($"Directory not found: {dir}");
            return 2;
        }

        var recursive = args.Contains("--recursive");
        var overwrite = args.Contains("--overwrite");
        var quiet = args.Contains("--quiet");
        var keepNeedtag = args.Contains("--keep-needtag");

        var concurrency = Clamp(ParseInt(GetArg(args, "--concurrency"), 8), 1, 64);
        var targetBytes = Clamp(ParseInt(GetArg(args, "--target-bytes"), 3_000_000), 400_000, 20_000_000);

        // 「少ないときに頑張る」基準。少なくても最終的には書く（欠損ゼロ）
        var minTags = Clamp(ParseInt(GetArg(args, "--min-tags"), 30), 5, 200);

        // 最終的に保存するタグ数上限（ノイズカット用）
        var maxTags = Clamp(ParseInt(GetArg(args, "--max-tags"), 80), 20, 300);

        // JoyTag server
        var joyUrl = GetArg(args, "--joy-url") ?? "http://127.0.0.1:7865";
        var joyRetries = Clamp(ParseInt(GetArg(args, "--joy-retries"), 3), 0, 10);
        var joyThresholdStart = ClampDouble(ParseDouble(GetArg(args, "--joy-threshold"), 0.40), 0.10, 0.90);
        var joyThresholdStep = ClampDouble(ParseDouble(GetArg(args, "--joy-threshold-step"), 0.05), 0.01, 0.20);

        // OpenAI optional
        var openAiModel = GetArg(args, "--model") ?? "gpt-4.1-mini";
        var openAiRetries = Clamp(ParseInt(GetArg(args, "--openai-retries"), 1), 0, 6);
        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY"); // 無ければ自動でスキップ

        // 既存txtスキップ。ただし“明らかに拒否/壊れ”は再生成して上書き
        var reprocessBadExisting = !args.Contains("--no-reprocess-bad-existing");

        var noOpenAi = args.Contains("--no-openai");

        if (noOpenAi) apiKey = null;

        if (!quiet) {
            Console.WriteLine($"Dir={dir}");
            Console.WriteLine($"Recursive={recursive} Overwrite={overwrite} KeepNeedtag={keepNeedtag}");
            Console.WriteLine($"Concurrency={concurrency} TargetBytes={targetBytes} MinTags={minTags} MaxTags={maxTags}");
            Console.WriteLine($"JoyUrl={joyUrl} JoyRetries={joyRetries} JoyThreshold={joyThresholdStart:0.00} Step={joyThresholdStep:0.00}");
            Console.WriteLine($"OpenAI={(string.IsNullOrWhiteSpace(apiKey) ? "OFF" : "ON")} Model={openAiModel} Retries={openAiRetries}");
            Console.WriteLine("Policy: Always write .txt even if tags are few.");
        }

        using var httpJoy = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
        using var httpOpenAI = new HttpClient { Timeout = TimeSpan.FromSeconds(180) };
        if (!string.IsNullOrWhiteSpace(apiKey))
            httpOpenAI.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

        var opt = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
        var images = Directory.EnumerateFiles(dir, "*.*", opt)
            .Where(p => ImageExts.Contains(Path.GetExtension(p), StringComparer.OrdinalIgnoreCase))
            .OrderBy(p => p, StringComparer.OrdinalIgnoreCase)
            .ToList();

        int ok = 0, skipped = 0, repaired = 0, low = 0, failed = 0;
        var errors = new ConcurrentBag<string>();

        var po = new ParallelOptions { MaxDegreeOfParallelism = concurrency };

        await Parallel.ForEachAsync(images, po, async (imagePath, ct) => {
            var txtPath = Path.ChangeExtension(imagePath, ".txt");
            var needPath = Path.ChangeExtension(imagePath, ".needtag");

            try {
                // 既存txtの扱い
                if (!overwrite && File.Exists(txtPath)) {
                    if (!reprocessBadExisting) {
                        Interlocked.Increment(ref skipped);
                        return;
                    }

                    var existing = await File.ReadAllTextAsync(txtPath, ct);
                    var existingNorm = NormalizeAndShape(existing, maxTags);

                    if (!LooksBad(existingNorm)) {
                        Interlocked.Increment(ref skipped);
                        return;
                    }

                    // 壊れてる既存txtは作り直す
                    TryDelete(txtPath);
                    Interlocked.Increment(ref repaired);
                }

                // 画像ロード＋必要なら3MBへ圧縮（OpenAI/HTTP送信用）
                var (mime, bytes) = await LoadAndMaybeCompressAsync(imagePath, targetBytes, ct);

                // 1) JoyTag server（基本これが主役）
                string tagsJoy = await JoyTagAutoRetryAsync(
                    httpJoy, joyUrl, bytes,
                    joyThresholdStart, joyThresholdStep,
                    joyRetries, minTags, ct);

                tagsJoy = NormalizeAndShape(tagsJoy, maxTags);

                // 2) OpenAI（任意の補強。JoyTagが弱い/少ないときだけ）
                string tagsOpen = "";
                if (!string.IsNullOrWhiteSpace(apiKey)) {
                    if (CountTags(tagsJoy) < minTags || LooksBad(tagsJoy)) {
                        tagsOpen = await OpenAiRetryAsync(httpOpenAI, openAiModel, mime, bytes, openAiRetries, ct);
                        tagsOpen = NormalizeAndShape(tagsOpen, maxTags);
                    }
                }

                // 3) マージ（“キャラ特徴”を落とさない方針）
                // - JoyTagを土台
                // - OpenAIが有益なら補完（ただしノイズもあるので強めに整形）
                string merged = MergePreferJoyThenOpen(tagsJoy, tagsOpen, maxTags);

                // 4) 最終：style prefix + hard prefix tags
                merged = EnsureHardPrefixes(merged);

                // 5) txt欠損ゼロ：空/極端に短いなら最低限を書く
                if (string.IsNullOrWhiteSpace(merged) || CountTags(merged) < 3)
                    merged = EnsureHardPrefixes(MinimalFallback);

                // 6) 書き込み
                await File.WriteAllTextAsync(txtPath, merged + Environment.NewLine, new UTF8Encoding(false), ct);

                // 7) needtag（低品質フラグ）
                bool isLow = CountTags(merged) < minTags || LooksBad(merged);
                if (isLow) {
                    Interlocked.Increment(ref low);
                    if (keepNeedtag)
                        await File.WriteAllTextAsync(needPath, "low tags; check later" + Environment.NewLine, new UTF8Encoding(false), ct);
                    else
                        TryDelete(needPath);
                } else {
                    TryDelete(needPath);
                }

                Interlocked.Increment(ref ok);

                if (!quiet)
                    Console.WriteLine($"[ok] {Path.GetFileName(imagePath)} tags={CountTags(merged)}");
            } catch (Exception ex) {
                Interlocked.Increment(ref failed);
                errors.Add($"[fail] {Path.GetFileName(imagePath)}: {ex.Message}");

                // 最悪でもtxtだけは作る（要望）
                try {
                    if (!File.Exists(txtPath))
                        await File.WriteAllTextAsync(txtPath, EnsureHardPrefixes(MinimalFallback) + Environment.NewLine, new UTF8Encoding(false), ct);

                    if (keepNeedtag)
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
    private static double ParseDouble(string? s, double fallback) => double.TryParse(s, out var v) ? v : fallback;

    private static int Clamp(int v, int lo, int hi) => Math.Max(lo, Math.Min(hi, v));
    private static double ClampDouble(double v, double lo, double hi) => Math.Max(lo, Math.Min(hi, v));

    private static void TryDelete(string path) {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }

    private static void PrintHelp() {
        Console.WriteLine(
@"ImageCaptioner (Final: JoyTag server + optional OpenAI, LoRA-friendly tags, always writes .txt)

Usage:
  ImageCaptioner <dir> [--recursive] [--overwrite] [--quiet]
    [--concurrency N] [--target-bytes BYTES]
    [--min-tags N] [--max-tags N]
    [--joy-url http://127.0.0.1:7865] [--joy-retries N]
    [--joy-threshold 0.40] [--joy-threshold-step 0.05]
    [--model MODEL] [--openai-retries N]
    [--keep-needtag]
    [--no-reprocess-bad-existing]

Notes:
  - ALWAYS writes .txt (even if tags are few).
  - Default: JoyTag server first; OpenAI only when JoyTag is weak (OPENAI_API_KEY required).
  - LoRA shaping: prefixes first, character features early, background/quality later, dedupe, drop junk.

Recommended:
  Start JoyTag server (GPU):
    python -m uvicorn joytag_server:app --host 127.0.0.1 --port 7865

  Run captioning:
    dotnet run -- ""D:\dataset"" --recursive --quiet --concurrency 12 --min-tags 30 --max-tags 80 --target-bytes 3000000 --keep-needtag --joy-url ""http://127.0.0.1:7865""
");
    }
}
