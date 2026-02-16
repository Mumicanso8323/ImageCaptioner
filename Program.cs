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
    private const int MaxSideResize = 2048;
    private static readonly string[] ImageExts = [".png", ".jpg", ".jpeg", ".webp"];

    private static string[] HardPrefixTags = [];
    private static HashSet<string> AllowlistTags = new(StringComparer.OrdinalIgnoreCase);
    private static HashSet<string> DenylistTags = new(StringComparer.OrdinalIgnoreCase);

    private static readonly HashSet<string> SubjectTokens = new(StringComparer.OrdinalIgnoreCase) {
        "1girl", "1boy", "solo", "multiple_girls", "multiple_boys", "2girls", "2boys", "girl", "boy"
    };

    private static readonly Regex CharacterNameRegex = new(@"^[a-z0-9]+(_[a-z0-9]+){1,3}$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex FaceHairEyesRegex = new(@"(hair|eyes|eyebrows|eyelashes|pupil|bangs|twintails|ponytail|braid|ahoge|blush|smile|frown|angry|surprised|open_mouth|closed_mouth|tears|looking_)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex OutfitRegex = new(@"(dress|skirt|shirt|jacket|coat|hoodie|sweater|uniform|kimono|swimsuit|socks|stockings|pantyhose|shoes|boots|gloves|hat|ribbon|bow|tie|neckwear|blazer|pants|shorts)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex PoseRegex = new(@"(pose|standing|sitting|lying|kneeling|crouching|walking|running|jumping|leaning|holding|raising|arms|hands|fingers|looking_at_viewer|looking_away|from_side|from_behind)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex BackgroundRegex = new(@"(indoors|outdoors|room|bedroom|street|city|school|classroom|park|forest|beach|sky|night|sunset|window|bed|chair|office|kitchen|bathroom)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex StyleRegex = new(@"(lighting|backlighting|rim_light|soft_light|hard_light|shadow|bloom|depth_of_field|bokeh|monochrome|highres|absurdres|masterpiece|best_quality|high_quality|detailed)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly string[] DropTagsContains = [
        "negative:", "positive:", "sorry", "i can't", "i cannot", "cannot provide", "unable to", "as an ai", "i'm not able", "i won’t", "i won't"
    ];

    private static readonly HashSet<string> BuiltinDenyTags = new(StringComparer.OrdinalIgnoreCase) {
        "watermark", "signature", "text", "logo", "jpeg_artifacts", "lowres", "worst_quality", "rating", "artist_name"
    };

    private enum TagBucket { Identity, FaceHairEyes, Outfit, PoseAction, Background, StyleQuality, Other }

    public static async Task<int> Main(string[] args) {
        try {
            if (args.Contains("--help") || args.Contains("-h")) {
                PrintHelp();
                return 0;
            }

            if (args.Contains("--self-check")) {
                var ok = RunSelfChecks();
                Console.WriteLine(ok ? "Self-check passed." : "Self-check failed.");
                return ExitWithErrorPause(ok ? 0 : 1);
            }

            var launchWizard = args.Length == 0 || args.Contains("--wizard") || (args.Length > 0 && args[0].StartsWith("-", StringComparison.Ordinal));
            var options = launchWizard ? RunWizard() : ParseOptions(args);
            if (options is null) return ExitWithErrorPause(2);

        HardPrefixTags = SplitTags(options.AutoPrefixTags).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        LoadAllowDenyLists();

        var minimalFallback = string.Join(", ", HardPrefixTags.Concat(SplitTags(options.MinimalSubjectTags)).Distinct(StringComparer.OrdinalIgnoreCase));

        if (!options.Quiet) {
            Console.WriteLine($"Dir={options.Dir} Recursive={options.Recursive} Overwrite={options.Overwrite} KeepNeedtag={options.KeepNeedtag}");
            Console.WriteLine($"Concurrency={options.Concurrency} TargetBytes={options.TargetBytes} TargetTags={options.TargetTags} MinTags={options.MinTags} MaxTags={options.MaxTags}");
            Console.WriteLine($"JoyUrl={options.JoyUrl} Threshold={options.JoyThreshold:0.00} Secondary={(options.JoyThresholdSecondary.HasValue ? options.JoyThresholdSecondary.Value.ToString("0.00", CultureInfo.InvariantCulture) : "OFF")}");
            Console.WriteLine($"OpenAI={(string.IsNullOrWhiteSpace(options.ApiKey) ? "OFF" : "ON")} Model={options.OpenAiModel} DisableOpenAI={options.DisableOpenAi}");
        }

            Process? autoStartedJoyTagProcess = null;
            if (options.AutoStartJoyTag) {
                var start = await StartOrReuseJoyTagServerAsync(options);
                if (start.ExitCode != 0) return ExitWithErrorPause(start.ExitCode);
                autoStartedJoyTagProcess = start.AutoStartedProcess;
                if (autoStartedJoyTagProcess is not null) RegisterJoyTagShutdownHandlers(autoStartedJoyTagProcess);
            }

            using var httpJoy = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
            using var httpOpenAI = new HttpClient { Timeout = TimeSpan.FromSeconds(180) };
            if (!string.IsNullOrWhiteSpace(options.ApiKey))
                httpOpenAI.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", options.ApiKey);

            var scanOpt = options.Recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
            var images = Directory.EnumerateFiles(options.Dir, "*.*", scanOpt)
                .Where(p => ImageExts.Contains(Path.GetExtension(p), StringComparer.OrdinalIgnoreCase))
                .OrderBy(p => p, StringComparer.OrdinalIgnoreCase)
                .ToList();

            int okCount = 0, skipped = 0, repaired = 0, low = 0, failed = 0;
            var errors = new ConcurrentBag<string>();
        var po = new ParallelOptions { MaxDegreeOfParallelism = options.Concurrency };

        await Parallel.ForEachAsync(images, po, async (imagePath, ct) => {
            var txtPath = Path.ChangeExtension(imagePath, ".txt");
            var needPath = Path.ChangeExtension(imagePath, ".needtag");
            try {
                if (!options.Overwrite && File.Exists(txtPath)) {
                    if (!options.ReprocessBadExisting) { Interlocked.Increment(ref skipped); return; }
                    var existing = await File.ReadAllTextAsync(txtPath, ct);
                    var existingNorm = NormalizeRawTagLine(existing);
                    if (!LooksBad(existingNorm)) { Interlocked.Increment(ref skipped); return; }
                    TryDelete(txtPath);
                    Interlocked.Increment(ref repaired);
                }

                var (mime, bytes) = await LoadAndMaybeCompressAsync(imagePath, options.TargetBytes, ct);

                var joyPrimary = await CallJoyTagAsync(httpJoy, options.JoyUrl, bytes, options.JoyThreshold, ct);
                var joyCandidates = joyPrimary.Tags;

                if (joyCandidates.Count < options.TargetTags / 2 && options.JoyThresholdSecondary.HasValue) {
                    var joySecondary = await CallJoyTagAsync(httpJoy, options.JoyUrl, bytes, options.JoyThresholdSecondary.Value, ct);
                    var secondaryTop = joySecondary.Tags.Take(Math.Max(8, options.TargetTags / 2));
                    joyCandidates = MergeCandidateTags(joyCandidates, secondaryTop);
                }

                var selectedFromJoy = SelectTags(joyCandidates, options);
                bool needOpenAi = !options.DisableOpenAi
                    && !string.IsNullOrWhiteSpace(options.ApiKey)
                    && (selectedFromJoy.Count < options.MinTags || MissingCoreBuckets(selectedFromJoy));

                var finalCandidates = joyCandidates;
                if (needOpenAi) {
                    var openText = await OpenAiRetryAsync(httpOpenAI, options.OpenAiModel, mime, bytes, options.OpenAiRetries, ct);
                    var openCandidates = ParseTagCandidatesFromLine(openText, null, sourceWeight: 0.7);
                    finalCandidates = MergeCandidateTags(joyCandidates, openCandidates);
                }

                var finalTags = SelectTags(finalCandidates, options);
                if (finalTags.Count < 3)
                    finalTags = SelectTags(ParseTagCandidatesFromLine(minimalFallback, null, 1.0), options);

                var line = string.Join(", ", finalTags);
                await File.WriteAllTextAsync(txtPath, line + Environment.NewLine, new UTF8Encoding(false), ct);

                bool isLow = finalTags.Count < options.MinTags || MissingCoreBuckets(finalTags);
                if (isLow) {
                    Interlocked.Increment(ref low);
                    if (options.KeepNeedtag)
                        await File.WriteAllTextAsync(needPath, "low tags; check later" + Environment.NewLine, new UTF8Encoding(false), ct);
                } else {
                    TryDelete(needPath);
                }

                Interlocked.Increment(ref okCount);
            } catch (Exception ex) {
                Interlocked.Increment(ref failed);
                errors.Add($"[fail] {Path.GetFileName(imagePath)}: {ex.Message}");
                try {
                    if (!File.Exists(txtPath)) {
                        var fb = SelectTags(ParseTagCandidatesFromLine(minimalFallback, null, 1.0), options);
                        await File.WriteAllTextAsync(txtPath, string.Join(", ", fb) + Environment.NewLine, new UTF8Encoding(false), ct);
                    }
                    if (options.KeepNeedtag)
                        await File.WriteAllTextAsync(needPath, "error; check later" + Environment.NewLine, new UTF8Encoding(false), ct);
                } catch { }
            }
        });

            Console.WriteLine($"Done. ok={okCount}, repaired={repaired}, skipped={skipped}, low={low}, failed={failed}");
            if (!errors.IsEmpty) {
                Console.WriteLine("---- Errors (summary) ----");
                foreach (var e in errors.Take(20)) Console.WriteLine(e);
                if (errors.Count > 20) Console.WriteLine($"(and {errors.Count - 20} more)");
            }

            return ExitWithErrorPause(failed == 0 ? 0 : 1);
        } catch (Exception ex) {
            Console.Error.WriteLine("Fatal error occurred.");
            Console.Error.WriteLine(ex.ToString());
            return ExitWithErrorPause(1);
        }
    }

    private static int ExitWithErrorPause(int exitCode) {
        if (exitCode == 0) return 0;
        PauseForError();
        return exitCode;
    }

    private static void PauseForError() {
        if (!Environment.UserInteractive) return;
        Console.WriteLine();
        Console.WriteLine("エラーで終了しました。Enterキーで終了します...");
        Console.ReadLine();
    }

    private sealed class TagCandidate {
        public string Tag { get; init; } = "";
        public double Score { get; init; }
        public int Order { get; init; }
    }

    private static List<TagCandidate> MergeCandidateTags(IEnumerable<TagCandidate> a, IEnumerable<TagCandidate> b) {
        var map = new Dictionary<string, TagCandidate>(StringComparer.OrdinalIgnoreCase);
        foreach (var c in a.Concat(b)) {
            if (!map.TryGetValue(c.Tag, out var old) || c.Score > old.Score) map[c.Tag] = c;
        }
        return map.Values.OrderByDescending(x => x.Score).ThenBy(x => x.Order).ToList();
    }

    private static (List<TagCandidate> Tags, bool HasScore) ParseJoyTags(JsonElement tagsEl) {
        var list = new List<TagCandidate>();
        bool hasScore = false;
        int order = 0;
        foreach (var t in tagsEl.EnumerateArray()) {
            if (t.ValueKind == JsonValueKind.String) {
                var s = NormalizeTag(t.GetString() ?? "");
                if (!string.IsNullOrWhiteSpace(s)) list.Add(new TagCandidate { Tag = s, Score = Math.Max(0.01, 1.0 - order * 0.001), Order = order++ });
                continue;
            }
            if (t.ValueKind != JsonValueKind.Object) continue;

            string? tag = null;
            double score = 0.5;
            if (t.TryGetProperty("tag", out var tagEl) && tagEl.ValueKind == JsonValueKind.String) tag = tagEl.GetString();
            else if (t.TryGetProperty("name", out var nameEl) && nameEl.ValueKind == JsonValueKind.String) tag = nameEl.GetString();

            if (t.TryGetProperty("score", out var scoreEl) && scoreEl.TryGetDouble(out var parsedScore)) { score = parsedScore; hasScore = true; }
            else if (t.TryGetProperty("confidence", out var confEl) && confEl.TryGetDouble(out var conf)) { score = conf; hasScore = true; }

            var n = NormalizeTag(tag ?? "");
            if (!string.IsNullOrWhiteSpace(n)) list.Add(new TagCandidate { Tag = n, Score = score, Order = order++ });
        }

        if (hasScore) list = list.OrderByDescending(x => x.Score).ThenBy(x => x.Order).ToList();
        return (list, hasScore);
    }

    private static async Task<(List<TagCandidate> Tags, bool HasScore)> CallJoyTagAsync(HttpClient http, string baseUrl, byte[] imageBytes, double threshold, CancellationToken ct) {
        var url = $"{baseUrl.TrimEnd('/')}/tag?threshold={threshold:0.00}";
        using var form = new MultipartFormDataContent();
        var file = new ByteArrayContent(imageBytes);
        file.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        form.Add(file, "file", "image.jpg");
        using var resp = await http.PostAsync(url, form, ct);
        if (!resp.IsSuccessStatusCode) return (new List<TagCandidate>(), false);

        var text = await resp.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(text);
        if (!doc.RootElement.TryGetProperty("tags", out var tagsEl) || tagsEl.ValueKind != JsonValueKind.Array)
            return (new List<TagCandidate>(), false);

        return ParseJoyTags(tagsEl);
    }

    private static async Task<string> OpenAiRetryAsync(HttpClient http, string model, string mime, byte[] imageBytes, int retries, CancellationToken ct) {
        string prompt =
            "Return exactly ONE line of comma-separated Danbooru-style tags for LoRA captioning. " +
            "Use 15-30 tags. Focus first on identity/subject, then face hair eyes expression, then outfit, then pose/action, then background. " +
            "Use minimal style/quality tags (0-2). Non-explicit only. " +
            "Output contract: lowercase, underscores, comma-separated tags, no prose.";

        string last = "";
        for (int i = 0; i <= retries; i++) {
            last = await CallResponsesApiOnceAsync(http, model, mime, imageBytes, prompt, ct);
            if (!string.IsNullOrWhiteSpace(last)) return last;
        }
        return last;
    }

    private static async Task<string> CallResponsesApiOnceAsync(HttpClient http, string model, string mime, byte[] imageBytes, string userPrompt, CancellationToken ct) {
        var base64 = Convert.ToBase64String(imageBytes);
        var payload = new {
            model,
            input = new object[] {
                new {
                    role = "user",
                    content = new object[] {
                        new { type = "input_text", text = userPrompt },
                        new { type = "input_image", image_url = $"data:{mime};base64,{base64}" }
                    }
                }
            },
            max_output_tokens = 220
        };

        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var resp = await http.PostAsync("https://api.openai.com/v1/responses", content, ct);
        if (!resp.IsSuccessStatusCode) return "";

        var respText = await resp.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(respText);
        if (doc.RootElement.TryGetProperty("output_text", out var ot) && ot.ValueKind == JsonValueKind.String)
            return ot.GetString() ?? "";

        if (doc.RootElement.TryGetProperty("output", out var output) && output.ValueKind == JsonValueKind.Array) {
            foreach (var item in output.EnumerateArray()) {
                if (!item.TryGetProperty("content", out var contents) || contents.ValueKind != JsonValueKind.Array) continue;
                foreach (var c in contents.EnumerateArray()) {
                    if (c.ValueKind != JsonValueKind.Object) continue;
                    if (c.TryGetProperty("type", out var t) && t.GetString() == "output_text" && c.TryGetProperty("text", out var te) && te.ValueKind == JsonValueKind.String)
                        return te.GetString() ?? "";
                }
            }
        }
        return "";
    }

    private static List<TagCandidate> ParseTagCandidatesFromLine(string raw, double? defaultScore, double sourceWeight) {
        var norm = NormalizeRawTagLine(raw);
        var list = new List<TagCandidate>();
        int i = 0;
        foreach (var t in SplitTags(norm)) {
            list.Add(new TagCandidate {
                Tag = t,
                Score = (defaultScore ?? Math.Max(0.05, 1.0 - i * 0.01)) * sourceWeight,
                Order = i++
            });
        }
        return list;
    }

    private static string NormalizeRawTagLine(string raw) {
        if (string.IsNullOrWhiteSpace(raw)) return "";
        var s = raw.Replace("\r\n", "\n").Trim();
        var pos = ExtractAfterPrefixLine(s, "POSITIVE:");
        if (!string.IsNullOrWhiteSpace(pos)) s = pos;
        s = CutAfterPrefix(s, "NEGATIVE:");
        s = StripCodeFences(s).Replace("\n", " ").Trim();
        if (LooksBad(s)) return "";

        var tags = SplitTags(s)
            .Select(NormalizeTag)
            .Where(t => t.Length >= 2)
            .Where(t => !ShouldDropTag(t))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        return string.Join(", ", tags);
    }

    private static List<string> SelectTags(List<TagCandidate> candidates, AppOptions options) {
        int target = Clamp(options.TargetTags, options.MinTags, options.MaxTags);
        var caps = new Dictionary<TagBucket, int> {
            [TagBucket.Identity] = 3,
            [TagBucket.FaceHairEyes] = 8,
            [TagBucket.Outfit] = 6,
            [TagBucket.PoseAction] = 5,
            [TagBucket.Background] = 4,
            [TagBucket.StyleQuality] = options.MaxQualityTags
        };

        var unique = new Dictionary<string, TagCandidate>(StringComparer.OrdinalIgnoreCase);
        foreach (var c in candidates) {
            var n = NormalizeTag(c.Tag);
            if (string.IsNullOrWhiteSpace(n) || ShouldDropTag(n)) continue;
            var normalized = new TagCandidate { Tag = n, Score = c.Score, Order = c.Order };
            if (!unique.TryGetValue(n, out var old) || normalized.Score > old.Score) unique[n] = normalized;
        }

        var sorted = unique.Values.OrderByDescending(x => x.Score).ThenBy(x => x.Order).ToList();
        var grouped = new Dictionary<TagBucket, List<TagCandidate>>();
        foreach (TagBucket b in Enum.GetValues<TagBucket>()) grouped[b] = [];
        foreach (var c in sorted) grouped[ClassifyTag(c.Tag)].Add(c);

        var result = new List<string>();
        var selected = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        AddIfPresent(HardPrefixTags);

        // force stable subject near front
        var subject = grouped[TagBucket.Identity].FirstOrDefault(x => SubjectTokens.Contains(x.Tag));
        if (subject is not null) AddOne(subject.Tag);

        // fill bucket quotas in priority order
        FillBucket(TagBucket.Identity, caps[TagBucket.Identity]);
        FillBucket(TagBucket.FaceHairEyes, caps[TagBucket.FaceHairEyes]);
        FillBucket(TagBucket.Outfit, caps[TagBucket.Outfit]);
        FillBucket(TagBucket.PoseAction, caps[TagBucket.PoseAction]);
        FillBucket(TagBucket.Background, caps[TagBucket.Background]);
        FillBucket(TagBucket.StyleQuality, caps[TagBucket.StyleQuality]);

        // fill remainder best-first, never over max
        foreach (var c in sorted) {
            if (result.Count >= target) break;
            AddOne(c.Tag);
        }

        if (result.Count > options.MaxTags)
            result = result.Take(options.MaxTags).ToList();

        return result;

        void FillBucket(TagBucket bucket, int cap) {
            int used = 0;
            foreach (var c in grouped[bucket]) {
                if (result.Count >= target || used >= cap) break;
                if (AddOne(c.Tag)) used++;
            }
        }

        void AddIfPresent(IEnumerable<string> tags) {
            foreach (var tag in tags.Select(NormalizeTag)) AddOne(tag);
        }

        bool AddOne(string tag) {
            if (string.IsNullOrWhiteSpace(tag) || result.Count >= options.MaxTags) return false;
            if (ShouldDropTag(tag) || !selected.Add(tag)) return false;
            result.Add(tag);
            return true;
        }
    }

    private static bool MissingCoreBuckets(List<string> tags) {
        bool hasFace = tags.Any(t => ClassifyTag(t) == TagBucket.FaceHairEyes);
        bool hasOutfit = tags.Any(t => ClassifyTag(t) == TagBucket.Outfit);
        return !hasFace || !hasOutfit;
    }

    private static TagBucket ClassifyTag(string tag) {
        if (SubjectTokens.Contains(tag) || IsLikelyCharacterName(tag)) return TagBucket.Identity;
        if (FaceHairEyesRegex.IsMatch(tag)) return TagBucket.FaceHairEyes;
        if (OutfitRegex.IsMatch(tag)) return TagBucket.Outfit;
        if (PoseRegex.IsMatch(tag)) return TagBucket.PoseAction;
        if (BackgroundRegex.IsMatch(tag)) return TagBucket.Background;
        if (StyleRegex.IsMatch(tag)) return TagBucket.StyleQuality;
        return TagBucket.Other;
    }

    private static bool IsLikelyCharacterName(string tag) {
        if (!CharacterNameRegex.IsMatch(tag)) return false;
        if (SubjectTokens.Contains(tag)) return false;
        if (FaceHairEyesRegex.IsMatch(tag) || OutfitRegex.IsMatch(tag) || PoseRegex.IsMatch(tag) || BackgroundRegex.IsMatch(tag) || StyleRegex.IsMatch(tag))
            return false;
        return true;
    }

    private static bool ShouldDropTag(string t) {
        var lower = t.ToLowerInvariant();
        if (DropTagsContains.Any(lower.Contains)) return true;
        if (lower.StartsWith("rating:")) return true;
        if (BuiltinDenyTags.Contains(lower)) return true;
        if (DenylistTags.Contains(lower)) return true;
        if (AllowlistTags.Count > 0 && !AllowlistTags.Contains(lower) && (lower.StartsWith("rating") || lower.Contains("watermark"))) return true;
        return false;
    }

    private static bool LooksBad(string s) {
        if (string.IsNullOrWhiteSpace(s)) return true;
        var lower = s.ToLowerInvariant();
        if (DropTagsContains.Any(lower.Contains)) return true;
        int commas = s.Count(c => c == ',');
        return commas < 2 && s.Length > 40;
    }

    private static string NormalizeTag(string t) {
        t = t.Trim().Trim('"', '\'', '`');
        t = Regex.Replace(t, @"[;。、\.]+$", "");
        t = Regex.Replace(t, @"\s+", "_");
        t = Regex.Replace(t, @"_+", "_");
        return t.ToLowerInvariant();
    }

    private static IEnumerable<string> SplitTags(string s) {
        if (string.IsNullOrWhiteSpace(s)) yield break;
        foreach (var part in s.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            var t = part.Trim();
            if (!string.IsNullOrWhiteSpace(t)) yield return t;
        }
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
            if (t.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) return t[prefix.Length..].Trim();
        }
        return "";
    }

    private static string CutAfterPrefix(string text, string prefix) {
        var idx = text.IndexOf(prefix, StringComparison.OrdinalIgnoreCase);
        return idx >= 0 ? text[..idx].Trim() : text;
    }

    private static void LoadAllowDenyLists() {
        AllowlistTags = LoadTagFile("allowlist.txt");
        DenylistTags = LoadTagFile("denylist.txt");
    }

    private static HashSet<string> LoadTagFile(string fileName) {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var path = Path.Combine(Environment.CurrentDirectory, fileName);
        if (!File.Exists(path)) return set;
        foreach (var line in File.ReadLines(path)) {
            var t = line.Trim();
            if (string.IsNullOrWhiteSpace(t) || t.StartsWith('#')) continue;
            set.Add(NormalizeTag(t));
        }
        return set;
    }

    private static async Task<(string mime, byte[] bytes)> LoadAndMaybeCompressAsync(string path, int targetBytes, CancellationToken ct) {
        var ext = Path.GetExtension(path).ToLowerInvariant();
        var orig = await File.ReadAllBytesAsync(path, ct);
        var origMime = ext switch {
            ".png" => "image/png",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".webp" => "image/webp",
            _ => "application/octet-stream"
        };
        if (orig.Length <= targetBytes) return (origMime, orig);

        using var img = Image.Load(orig);
        var maxSide = Math.Max(img.Width, img.Height);
        if (maxSide > MaxSideResize) {
            var scale = (double)MaxSideResize / maxSide;
            img.Mutate(x => x.Resize((int)(img.Width * scale), (int)(img.Height * scale)));
        }

        int lo = 35, hi = 92;
        byte[] best = Array.Empty<byte>();
        while (lo <= hi) {
            int q = (lo + hi) / 2;
            using var ms = new MemoryStream();
            img.Save(ms, new JpegEncoder { Quality = q });
            var b = ms.ToArray();
            best = b;
            if (b.Length > targetBytes) hi = q - 1; else lo = q + 1;
        }
        return ("image/jpeg", best);
    }

    private static AppOptions? ParseOptions(string[] args) {
        var dir = args[0];
        if (!Directory.Exists(dir)) { Console.Error.WriteLine($"Directory not found: {dir}"); return null; }

        var options = new AppOptions {
            Dir = dir,
            Recursive = args.Contains("--recursive"),
            Overwrite = args.Contains("--overwrite"),
            Quiet = args.Contains("--quiet"),
            KeepNeedtag = args.Contains("--keep-needtag"),
            Concurrency = Clamp(ParseInt(GetArg(args, "--concurrency"), 8), 1, 64),
            TargetBytes = Clamp(ParseInt(GetArg(args, "--target-bytes"), 3_000_000), 400_000, 20_000_000),
            MinTags = Clamp(ParseInt(GetArg(args, "--min-tags"), 15), 3, 200),
            MaxTags = Clamp(ParseInt(GetArg(args, "--max-tags"), 35), 10, 200),
            TargetTags = Clamp(ParseInt(GetArg(args, "--target-tags"), 25), 10, 80),
            JoyUrl = GetArg(args, "--joy-url") ?? "http://127.0.0.1:7865",
            JoyThreshold = ClampDouble(ParseDouble(GetArg(args, "--joy-threshold"), 0.45), 0.10, 0.95),
            JoyThresholdSecondary = ParseOptionalDouble(GetArg(args, "--joy-threshold-secondary"), 0.35),
            MaxQualityTags = Clamp(ParseInt(GetArg(args, "--max-quality-tags"), 2), 0, 10),
            OpenAiModel = GetArg(args, "--model") ?? "gpt-4.1-mini",
            OpenAiRetries = Clamp(ParseInt(GetArg(args, "--openai-retries"), 1), 0, 6),
            ReprocessBadExisting = !args.Contains("--no-reprocess-bad-existing"),
            AutoPrefixTags = GetArg(args, "--auto-prefix-tags") ?? "",
            MinimalSubjectTags = GetArg(args, "--minimal-subject-tags") ?? "",
            AutoStartJoyTag = args.Contains("--auto-start-joytag"),
            JoyTagPythonExe = GetArg(args, "--joytag-python"),
            JoyTagWorkingDir = GetArg(args, "--joytag-dir"),
            DisableOpenAi = args.Contains("--disable-openai") || args.Contains("--no-openai")
        };

        if (options.MaxTags < options.MinTags) options.MaxTags = options.MinTags;
        options.TargetTags = Clamp(options.TargetTags, options.MinTags, options.MaxTags);

        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        if (options.DisableOpenAi) apiKey = null;
        options.ApiKey = apiKey;

        if (!HasUserDefinedMinimumTags(options)) return null;
        return options;
    }

    private static AppOptions? RunWizard() {
        var saved = LoadWizardConfig();
        Console.WriteLine("=== ImageCaptioner Wizard ===");

        var dir = PromptString("画像フォルダ", saved.LastDir);
        if (string.IsNullOrWhiteSpace(dir) || !Directory.Exists(dir)) return null;

        var options = new AppOptions {
            Dir = dir,
            Recursive = PromptBoolByEmpty("再帰処理する？", saved.Recursive),
            Overwrite = PromptBoolByEmpty("既存txtも上書きする？", saved.Overwrite),
            Quiet = PromptBoolByEmpty("ログ少なめ？", saved.Quiet),
            KeepNeedtag = PromptBoolByEmpty(".needtagを残す？", saved.KeepNeedtag),
            ReprocessBadExisting = true,
            Concurrency = PromptInt("並列数", saved.Concurrency, 1, 64),
            MinTags = PromptInt("最小タグ数", saved.MinTags, 3, 200),
            MaxTags = PromptInt("最大タグ数", saved.MaxTags, 10, 200),
            TargetTags = PromptInt("目標タグ数", saved.TargetTags, 10, 80),
            TargetBytes = PromptInt("送信用ターゲットサイズ(bytes)", saved.TargetBytes, 400_000, 20_000_000),
            JoyUrl = PromptString("JoyTag URL", saved.JoyUrl),
            JoyThreshold = PromptDouble("JoyTag threshold", saved.JoyThreshold, 0.10, 0.95),
            JoyThresholdSecondary = PromptOptionalDouble("JoyTag secondary threshold(空で無効)", saved.JoyThresholdSecondary, 0.10, 0.95),
            MaxQualityTags = PromptInt("quality/styleタグ上限", saved.MaxQualityTags, 0, 10),
            OpenAiModel = saved.OpenAiModel,
            OpenAiRetries = saved.OpenAiRetries,
            DisableOpenAi = PromptBoolByEmpty("OpenAI無効化？", saved.DisableOpenAi),
            AutoPrefixTags = PromptString("常に先頭へ付けるタグ", saved.AutoPrefixTags),
            MinimalSubjectTags = PromptString("失敗時フォールバックタグ", saved.MinimalSubjectTags),
            AutoStartJoyTag = PromptBoolByEmpty("JoyTag自動起動？", saved.AutoStartJoyTag),
            JoyTagWorkingDir = PromptString("JoyTagフォルダ", saved.JoyTagWorkingDir),
            JoyTagPythonExe = PromptString("JoyTag python.exe", saved.JoyTagPythonExe)
        };

        options.TargetTags = Clamp(options.TargetTags, options.MinTags, options.MaxTags);

        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        if (options.DisableOpenAi) apiKey = null;
        options.ApiKey = apiKey;

        if (!HasUserDefinedMinimumTags(options)) return null;

        SaveWizardConfig(new WizardConfig {
            LastDir = options.Dir,
            JoyUrl = options.JoyUrl,
            Concurrency = options.Concurrency,
            MinTags = options.MinTags,
            MaxTags = options.MaxTags,
            TargetTags = options.TargetTags,
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
            JoyThreshold = options.JoyThreshold,
            JoyThresholdSecondary = options.JoyThresholdSecondary,
            OpenAiModel = options.OpenAiModel,
            OpenAiRetries = options.OpenAiRetries,
            MaxQualityTags = options.MaxQualityTags,
            DisableOpenAi = options.DisableOpenAi
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
        if (string.IsNullOrWhiteSpace(options.JoyTagPythonExe) || string.IsNullOrWhiteSpace(options.JoyTagWorkingDir)) {
            Console.Error.WriteLine("[error] --auto-start-joytag requires --joytag-python and --joytag-dir.");
            return (2, null);
        }

        var psi = new ProcessStartInfo {
            FileName = options.JoyTagPythonExe,
            WorkingDirectory = options.JoyTagWorkingDir,
            Arguments = "app.py",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        var process = Process.Start(psi);
        if (process is null) return (2, null);

        for (int i = 0; i < 40; i++) {
            if (await IsJoyTagAliveAsync(options.JoyUrl, CancellationToken.None)) return (0, process);
            await Task.Delay(500);
        }

        Console.Error.WriteLine("[error] JoyTag did not become ready in time.");
        return (2, null);
    }

    private static async Task<bool> IsJoyTagAliveAsync(string baseUrl, CancellationToken ct) {
        try {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
            using var resp = await http.GetAsync(baseUrl.TrimEnd('/') + "/", ct);
            return resp.IsSuccessStatusCode || (int)resp.StatusCode < 500;
        } catch {
            return false;
        }
    }

    private static void RegisterJoyTagShutdownHandlers(Process process) {
        void Kill() {
            try { if (!process.HasExited) process.Kill(entireProcessTree: true); } catch { }
        }
        Console.CancelKeyPress += (_, e) => { e.Cancel = false; Kill(); };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => Kill();
    }

    private static bool RunSelfChecks() {
        var options = new AppOptions {
            MinTags = 15,
            MaxTags = 35,
            TargetTags = 25,
            MaxQualityTags = 2,
            AutoPrefixTags = "trigger_token",
            MinimalSubjectTags = "1girl"
        };
        HardPrefixTags = SplitTags(options.AutoPrefixTags).ToArray();

        var candidates = ParseTagCandidatesFromLine("1girl, solo, blue_hair, long_hair, blue_eyes, smile, school_uniform, standing, classroom, masterpiece, best_quality, highres, watermark", null, 1.0);
        var tags = SelectTags(candidates, options);

        bool prefixFirst = tags.Count > 0 && tags[0] == "trigger_token";
        bool hasSubjectEarly = tags.Take(3).Contains("1girl");
        bool maxQualityRespected = tags.Count(t => ClassifyTag(t) == TagBucket.StyleQuality) <= 2;
        bool denyDropped = !tags.Contains("watermark");

        return prefixFirst && hasSubjectEarly && maxQualityRespected && denyDropped;
    }

    private static string? GetArg(string[] args, string name) {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i].Equals(name, StringComparison.OrdinalIgnoreCase))
                return args[i + 1];
        return null;
    }

    private static int ParseInt(string? s, int fallback) => int.TryParse(s, out var v) ? v : fallback;
    private static double ParseDouble(string? s, double fallback) => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : fallback;
    private static double? ParseOptionalDouble(string? s, double fallback) {
        if (string.IsNullOrWhiteSpace(s)) return fallback;
        if (s.Equals("off", StringComparison.OrdinalIgnoreCase) || s.Equals("none", StringComparison.OrdinalIgnoreCase)) return null;
        return ParseDouble(s, fallback);
    }
    private static int Clamp(int v, int lo, int hi) => Math.Max(lo, Math.Min(hi, v));
    private static double ClampDouble(double v, double lo, double hi) => Math.Max(lo, Math.Min(hi, v));
    private static void TryDelete(string path) { try { if (File.Exists(path)) File.Delete(path); } catch { } }

    private static string PromptString(string label, string fallback) {
        Console.Write($"{label} [{fallback}]: ");
        var input = Console.ReadLine();
        return string.IsNullOrWhiteSpace(input) ? fallback : input.Trim();
    }
    private static bool PromptBoolByEmpty(string label, bool fallback) {
        Console.Write($"{label} (Enter=前回値, 現在値: {fallback}) [y/n]: ");
        var input = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(input)) return fallback;
        var t = input.Trim().ToLowerInvariant();
        return t is "1" or "true" or "t" or "yes" or "y";
    }
    private static int PromptInt(string label, int fallback, int min, int max) => Clamp(ParseInt(PromptString(label, fallback.ToString(CultureInfo.InvariantCulture)), fallback), min, max);
    private static double PromptDouble(string label, double fallback, double min, double max) => ClampDouble(ParseDouble(PromptString(label, fallback.ToString("0.00", CultureInfo.InvariantCulture)), fallback), min, max);
    private static double? PromptOptionalDouble(string label, double? fallback, double min, double max) {
        var fb = fallback?.ToString("0.00", CultureInfo.InvariantCulture) ?? "off";
        var input = PromptString(label, fb);
        if (string.IsNullOrWhiteSpace(input)) return fallback;
        if (input.Equals("off", StringComparison.OrdinalIgnoreCase) || input.Equals("none", StringComparison.OrdinalIgnoreCase)) return null;
        return ClampDouble(ParseDouble(input, fallback ?? 0.35), min, max);
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
            return JsonSerializer.Deserialize<WizardConfig>(json) ?? new WizardConfig();
        } catch {
            return new WizardConfig();
        }
    }
    private static void SaveWizardConfig(WizardConfig config) {
        try {
            var path = GetWizardConfigPath();
            var json = JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(path, json, new UTF8Encoding(false));
        } catch { }
    }

    private sealed class AppOptions {
        public string Dir { get; set; } = "";
        public bool Recursive { get; set; } = true;
        public bool Overwrite { get; set; }
        public bool Quiet { get; set; } = true;
        public bool KeepNeedtag { get; set; } = true;
        public int Concurrency { get; set; } = 12;
        public int TargetBytes { get; set; } = 3_000_000;
        public int MinTags { get; set; } = 15;
        public int MaxTags { get; set; } = 35;
        public int TargetTags { get; set; } = 25;
        public string JoyUrl { get; set; } = "http://127.0.0.1:7865";
        public double JoyThreshold { get; set; } = 0.45;
        public double? JoyThresholdSecondary { get; set; } = 0.35;
        public int MaxQualityTags { get; set; } = 2;
        public string OpenAiModel { get; set; } = "gpt-4.1-mini";
        public int OpenAiRetries { get; set; } = 1;
        public string? ApiKey { get; set; }
        public bool DisableOpenAi { get; set; }
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
        public int MinTags { get; set; } = 15;
        public int MaxTags { get; set; } = 35;
        public int TargetTags { get; set; } = 25;
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
        public double JoyThreshold { get; set; } = 0.45;
        public double? JoyThresholdSecondary { get; set; } = 0.35;
        public string OpenAiModel { get; set; } = "gpt-4.1-mini";
        public int OpenAiRetries { get; set; } = 1;
        public int MaxQualityTags { get; set; } = 2;
        public bool DisableOpenAi { get; set; }
    }

    private static void PrintHelp() {
        Console.WriteLine(
@"ImageCaptioner

Usage:
  ImageCaptioner
  ImageCaptioner <dir> [--recursive] [--overwrite] [--quiet]
    [--concurrency N] [--target-bytes BYTES]
    [--min-tags N] [--max-tags N] [--target-tags N]
    [--joy-url http://127.0.0.1:7865]
    [--joy-threshold 0.45] [--joy-threshold-secondary 0.35|off]
    [--max-quality-tags N]
    [--disable-openai|--no-openai] [--model MODEL] [--openai-retries N]
    [--keep-needtag] [--no-reprocess-bad-existing]
    [--auto-prefix-tags <comma,separated,tags>]
    [--minimal-subject-tags <comma,separated,tags>]
    [--auto-start-joytag --joytag-dir D:\tools\joytag --joytag-python D:\tools\joytag\venv\Scripts\python.exe]
    [--self-check]

Notes:
  - Caption output is always one line: tag1, tag2, ...
  - Default caption range is short and stable for LoRA (15-35 tags, target 25).
  - JoyTag is called once (optional secondary threshold once); no threshold chasing loop.
  - OpenAI is optional fallback only when core buckets are missing or user enables it.
  - allowlist.txt / denylist.txt in working directory are loaded at startup (denylist wins).");
    }
}
