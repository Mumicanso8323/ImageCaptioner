using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Net.Sockets;
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
    private static HashSet<string> KnownTags = new(StringComparer.OrdinalIgnoreCase);
    private static bool KnownTagDictionaryFound;
    private static string? KnownTagDictionaryPath;

    private static readonly HashSet<string> SubjectTokens = new(StringComparer.OrdinalIgnoreCase) {
        "1girl", "1boy", "solo", "multiple_girls", "multiple_boys", "2girls", "2boys", "girl", "boy"
    };
    private static readonly HashSet<string> MaleSubjectTokens = new(StringComparer.OrdinalIgnoreCase) {
        "boy", "1boy", "2boys", "3boys", "4boys", "multiple_boys", "male_focus", "yaoi"
    };
    private static readonly HashSet<string> BannedFinalTags = new(StringComparer.OrdinalIgnoreCase) {
        "1boy", "boy", "multiple_boys", "2boys", "3boys", "4boys", "male_focus", "yaoi", "male"
    };
    private static readonly object ReviewLogLock = new();
    private static bool ActiveStrictKnownTags;
    private const int MaxTotalTags = 42;
    private static readonly Regex ValidTagRegex = new("^[a-z0-9][a-z0-9_()!'/-]*$", RegexOptions.Compiled);
    private static readonly HashSet<string> ForceAllowTags = new(StringComparer.OrdinalIgnoreCase);
    private static ProfileConfig? ActiveProfile;
    private static readonly HashSet<string> SafeModeRejectTags = new(StringComparer.OrdinalIgnoreCase) {
        "nsfw", "nude", "naked", "nipples", "breasts", "pussy", "cameltoe", "sex", "penis", "cum", "orgasm", "masturbation", "fellatio", "anal", "vaginal", "hentai", "explicit", "porn", "bdsm", "futanari"
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

            var useLast = args.Contains("--use-last");
            var launchWizard = args.Length == 0 || args.Contains("--wizard");
            var options = launchWizard ? RunWizard() : ParseOptions(args, useLast);
            if (options is null) return ExitWithErrorPause(2);
            ActiveStrictKnownTags = options.StrictKnownTags;

        HardPrefixTags = SplitTags(options.AutoPrefixTags).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        LoadAllowDenyLists();
        LoadKnownTagDictionary(options);
        ActiveProfile = options.NoProfile ? null : LoadProfile(options.Profile, options);
        BuildForceAllowTags(options, ActiveProfile);

        var minimalFallback = string.Join(", ", HardPrefixTags.Concat(SplitTags(options.MinimalSubjectTags)).Distinct(StringComparer.OrdinalIgnoreCase));

        if (!options.Quiet) {
            Console.WriteLine($"Dir={options.Dir} Recursive={options.Recursive} Overwrite={options.Overwrite} KeepNeedtag={options.KeepNeedtag}");
            Console.WriteLine($"Concurrency={options.Concurrency} TargetBytes={options.TargetBytes} MaxTotalTags={MaxTotalTags}");
            Console.WriteLine($"JoyUrl={options.JoyUrl} Threshold={options.JoyThreshold:0.00} Secondary={(options.JoyThresholdSecondary.HasValue ? options.JoyThresholdSecondary.Value.ToString("0.00", CultureInfo.InvariantCulture) : "OFF")}");
            Console.WriteLine($"OpenAI={(string.IsNullOrWhiteSpace(options.ApiKey) ? "OFF" : "ON")} Model={options.OpenAiModel} DisableOpenAI={options.DisableOpenAi}");
            Console.WriteLine($"KnownTagDictionary={(KnownTagDictionaryFound ? $"FOUND ({KnownTags.Count}) at {KnownTagDictionaryPath}" : "NOT FOUND")}");
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

                var joyPrimary = await CallJoyTagAsync(httpJoy, options.JoyUrl, bytes, options.JoyThreshold, TagSource.PrimaryJoy, ct);
                var joyCandidates = joyPrimary.Tags;

                // Keep thresholds deterministic and do not repeatedly lower threshold.
                if (options.JoyThresholdSecondary.HasValue) {
                    var joySecondary = await CallJoyTagAsync(httpJoy, options.JoyUrl, bytes, options.JoyThresholdSecondary.Value, TagSource.SecondaryJoy, ct);
                    joyCandidates = MergeCandidateTags(joyCandidates, LimitSecondaryTags(joyPrimary.Tags, joySecondary.Tags, options.SecondaryNewTagsCap));
                }

                var selectedFromJoy = SelectTags(joyCandidates, options);
                bool needOpenAi = !options.DisableOpenAi
                    && !string.IsNullOrWhiteSpace(options.ApiKey)
                    && MissingCoreBuckets(selectedFromJoy.Tags);

                var finalCandidates = joyCandidates;
                if (needOpenAi) {
                    var openText = await OpenAiRetryAsync(httpOpenAI, options.OpenAiModel, mime, bytes, options.OpenAiRetries, ct);
                    var openCandidates = ParseTagCandidatesFromLine(openText, null, sourceWeight: 0.7, source: TagSource.OpenAI);
                    finalCandidates = MergeCandidateTags(joyCandidates, openCandidates);
                }

                var finalSelection = SelectTags(finalCandidates, options);
                var finalTags = finalSelection.Tags;
                if (finalTags.Count < 3)
                    finalTags = SelectTags(ParseTagCandidatesFromLine(minimalFallback, null, 1.0, TagSource.OpenAI), options).Tags;
                var unknownFinalTags = GetUnknownTags(finalTags);

                var line = string.Join(", ", finalTags);
                await File.WriteAllTextAsync(txtPath, line + Environment.NewLine, new UTF8Encoding(false), ct);

                bool isLow = MissingCoreBuckets(finalTags);
                if (isLow) {
                    Interlocked.Increment(ref low);
                    if (options.KeepNeedtag)
                        await File.WriteAllTextAsync(needPath, "low tags; short caption or missing core; check later" + Environment.NewLine, new UTF8Encoding(false), ct);
                } else {
                    TryDelete(needPath);
                }

                var review = BuildReviewRecord(imagePath, txtPath, joyCandidates, finalSelection, options, unknownFinalTags, isLow);
                var rejectReason = options.SafeMode ? DetectSafeModeRejectReason(finalTags) : null;
                if (!string.IsNullOrWhiteSpace(rejectReason)) {
                    HandleReject(imagePath, txtPath, options, rejectReason!);
                }

                AppendAudit(options.Dir, Path.GetFileName(imagePath), finalTags, finalSelection.DroppedTagsWithReason, rejectReason, KnownTagDictionaryPath, options.NoProfile ? "none" : options.Profile);

                if (review is not null) {
                    AppendReviewLog(options.Dir, review);
                    if (!string.IsNullOrWhiteSpace(options.ReviewCopyDir)) {
                        TryCopyReviewArtifacts(options.ReviewCopyDir!, imagePath, txtPath);
                    }
                }

                Interlocked.Increment(ref okCount);
            } catch (Exception ex) {
                Interlocked.Increment(ref failed);
                errors.Add($"[fail] {Path.GetFileName(imagePath)}: {ex.Message}");
                try {
                    if (!File.Exists(txtPath)) {
                        var fb = SelectTags(ParseTagCandidatesFromLine(minimalFallback, null, 1.0, TagSource.OpenAI), options).Tags;
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

            SaveWizardConfig(ToWizardConfig(options));
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
        public TagSource Source { get; init; } = TagSource.PrimaryJoy;
    }

    private enum TagSource { PrimaryJoy, SecondaryJoy, OpenAI }

    private const int DefaultStyleTagCap = 2;
    private const int MaxStyleTagCap = 4;

    private sealed class SelectionResult {
        public List<string> Tags { get; init; } = [];
        public List<string> DroppedMaleSubjectTags { get; init; } = [];
        public List<string> DroppedTagsWithReason { get; init; } = [];
    }

    private sealed class ProfileConfig {
        public string Trigger { get; set; } = "sgrui";
        public List<string> AlwaysAdd { get; set; } = [];
        public List<string> PreferKeep { get; set; } = [];
    }

    private sealed class ReviewRecord {
        public string ImagePath { get; init; } = "";
        public string CaptionPath { get; init; } = "";
        public List<string> Reasons { get; init; } = [];
        public List<string> BannedTokensDetected { get; init; } = [];
        public List<string> DroppedTags { get; init; } = [];
        public int FinalTagCount { get; init; }
        public bool ShortCaption { get; init; }
        public string ThresholdsUsed { get; init; } = "";
        public List<string> TopJoyTags { get; init; } = [];
        public List<string> UnknownFinalTags { get; init; } = [];
        public bool KnownTagDictionaryFound { get; init; }
        public int KnownTagDictionaryCount { get; init; }
        public string KnownTagDictionaryPath { get; init; } = "";
    }

    private static List<TagCandidate> MergeCandidateTags(IEnumerable<TagCandidate> a, IEnumerable<TagCandidate> b) {
        var map = new Dictionary<string, TagCandidate>(StringComparer.OrdinalIgnoreCase);
        foreach (var c in a.Concat(b)) {
            if (!map.TryGetValue(c.Tag, out var old)) {
                map[c.Tag] = c;
                continue;
            }

            var bestScore = c.Score > old.Score ? c : old;
            var earliestOrder = Math.Min(old.Order, c.Order);
            var source = PreferSource(old.Source, c.Source, Math.Abs(old.Score - c.Score) <= 0.03 ? old.Score : bestScore.Score, Math.Abs(old.Score - c.Score) <= 0.03 ? c.Score : bestScore.Score);
            map[c.Tag] = new TagCandidate {
                Tag = bestScore.Tag,
                Score = Math.Max(old.Score, c.Score),
                Order = earliestOrder,
                Source = source
            };
        }
        return map.Values.OrderByDescending(x => x.Score).ThenBy(x => SourceRank(x.Source)).ThenBy(x => x.Order).ToList();
    }

    private static (List<TagCandidate> Tags, bool HasScore) ParseJoyTags(JsonElement tagsEl, TagSource source) {
        var list = new List<TagCandidate>();
        bool hasScore = false;
        int order = 0;
        foreach (var t in tagsEl.EnumerateArray()) {
            if (t.ValueKind == JsonValueKind.String) {
                var s = NormalizeTag(t.GetString() ?? "");
                if (!string.IsNullOrWhiteSpace(s)) list.Add(new TagCandidate { Tag = s, Score = Math.Max(0.01, 1.0 - order * 0.001), Order = order++, Source = source });
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
            if (!string.IsNullOrWhiteSpace(n)) list.Add(new TagCandidate { Tag = n, Score = score, Order = order++, Source = source });
        }

        if (hasScore) list = list.OrderByDescending(x => x.Score).ThenBy(x => x.Order).ToList();
        return (list, hasScore);
    }

    private static async Task<(List<TagCandidate> Tags, bool HasScore)> CallJoyTagAsync(HttpClient http, string baseUrl, byte[] imageBytes, double threshold, TagSource source, CancellationToken ct) {
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

        return ParseJoyTags(tagsEl, source);
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

    private static List<TagCandidate> ParseTagCandidatesFromLine(string raw, double? defaultScore, double sourceWeight, TagSource source = TagSource.OpenAI) {
        var norm = NormalizeRawTagLine(raw);
        var list = new List<TagCandidate>();
        int i = 0;
        foreach (var t in SplitTags(norm)) {
            list.Add(new TagCandidate {
                Tag = t,
                Score = (defaultScore ?? Math.Max(0.05, 1.0 - i * 0.01)) * sourceWeight,
                Order = i++,
                Source = source
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

    private static SelectionResult SelectTags(List<TagCandidate> candidates, AppOptions options) {
        var styleQualityCap = DetermineStyleQualityCap(candidates);
        var caps = new Dictionary<TagBucket, int> {
            [TagBucket.Identity] = 3,
            [TagBucket.FaceHairEyes] = 10,
            [TagBucket.Outfit] = 8,
            [TagBucket.PoseAction] = 6,
            [TagBucket.Background] = 4,
            [TagBucket.StyleQuality] = styleQualityCap
        };

        var droppedMale = candidates
            .Select(c => NormalizeTag(c.Tag))
            .Where(t => !string.IsNullOrWhiteSpace(t) && BannedFinalTags.Contains(t))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var droppedReasons = new List<string>();
        var unique = new Dictionary<string, TagCandidate>(StringComparer.OrdinalIgnoreCase);
        foreach (var c in candidates) {
            var n = NormalizeTag(c.Tag);
            if (string.IsNullOrWhiteSpace(n)) continue;
            if (!IsValidTag(n)) { droppedReasons.Add($"{n}:invalid_tag"); continue; }
            if (ShouldDropTag(n)) { droppedReasons.Add($"{n}:policy_drop"); continue; }

            var normalized = new TagCandidate { Tag = n, Score = c.Score, Order = c.Order, Source = c.Source };
            if (!unique.TryGetValue(n, out var old)) {
                unique[n] = normalized;
                continue;
            }

            var prefer = normalized.Score > old.Score ? normalized : old;
            if (Math.Abs(normalized.Score - old.Score) <= 0.03) {
                prefer = SourceRank(normalized.Source) < SourceRank(old.Source) ? normalized : old;
            }
            unique[n] = new TagCandidate {
                Tag = prefer.Tag,
                Score = Math.Max(normalized.Score, old.Score),
                Order = Math.Min(normalized.Order, old.Order),
                Source = prefer.Source
            };
        }

        var sorted = unique.Values
            .OrderByDescending(x => x.Score)
            .ThenBy(x => SourceRank(x.Source))
            .ThenBy(x => x.Order)
            .ToList();

        var grouped = new Dictionary<TagBucket, List<TagCandidate>>();
        foreach (TagBucket b in Enum.GetValues<TagBucket>()) grouped[b] = [];
        foreach (var c in sorted) grouped[ClassifyTag(c.Tag)].Add(c);

        var result = new List<string>();
        var selected = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        AddOne(options.Trigger);
        AddOne("1girl");
        if (ActiveProfile is not null) {
            foreach (var t in ActiveProfile.AlwaysAdd.Select(NormalizeTag)) AddOne(t);
        }

        FillBucket(TagBucket.Identity, caps[TagBucket.Identity]);
        FillBucket(TagBucket.FaceHairEyes, caps[TagBucket.FaceHairEyes]);
        FillBucket(TagBucket.Outfit, caps[TagBucket.Outfit]);
        FillBucket(TagBucket.PoseAction, caps[TagBucket.PoseAction]);
        FillBucket(TagBucket.Background, caps[TagBucket.Background]);
        FillBucket(TagBucket.StyleQuality, caps[TagBucket.StyleQuality]);

        foreach (var c in sorted) {
            if (result.Count >= MaxTotalTags) break;
            AddOne(c.Tag);
        }

        return new SelectionResult { Tags = result, DroppedMaleSubjectTags = droppedMale, DroppedTagsWithReason = droppedReasons.Distinct(StringComparer.OrdinalIgnoreCase).ToList() };

        void FillBucket(TagBucket bucket, int cap) {
            int used = 0;
            foreach (var c in grouped[bucket]) {
                if (result.Count >= MaxTotalTags || used >= cap) break;
                if (AddOne(c.Tag)) used++;
            }
        }

        bool AddOne(string tag) {
            var n = NormalizeTag(tag);
            if (string.IsNullOrWhiteSpace(n) || result.Count >= MaxTotalTags) return false;
            if (!IsValidTag(n)) return false;
            if (ShouldDropTag(n) || !selected.Add(n)) return false;
            result.Add(n);
            return true;
        }
    }

    private static int DetermineStyleQualityCap(List<TagCandidate> candidates) {
        var strongStyleCandidates = candidates
            .Where(c => ClassifyTag(c.Tag) == TagBucket.StyleQuality && c.Score >= 0.60)
            .Select(c => c.Tag)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();

        return Clamp(strongStyleCandidates, DefaultStyleTagCap, MaxStyleTagCap);
    }

    private static int SourceRank(TagSource source) => source switch {
        TagSource.PrimaryJoy => 0,
        TagSource.SecondaryJoy => 1,
        TagSource.OpenAI => 2,
        _ => 3
    };

    private static TagSource PreferSource(TagSource a, TagSource b, double scoreA, double scoreB) {
        if (Math.Abs(scoreA - scoreB) > 0.03) return scoreA >= scoreB ? a : b;
        return SourceRank(a) <= SourceRank(b) ? a : b;
    }

    private static List<TagCandidate> LimitSecondaryTags(List<TagCandidate> primary, List<TagCandidate> secondary, int cap) {
        if (cap <= 0 || secondary.Count == 0) return [];
        var primarySet = primary.Select(x => x.Tag).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var accepted = new List<TagCandidate>();
        foreach (var c in secondary.OrderByDescending(x => x.Score).ThenBy(x => x.Order)) {
            if (primarySet.Contains(c.Tag)) {
                accepted.Add(c);
                continue;
            }
            if (cap == 0) continue;
            accepted.Add(c);
            cap--;
        }
        return accepted;
    }

    private static List<string> GetUnknownTags(IEnumerable<string> tags) {
        if (!KnownTagDictionaryFound || KnownTags.Count == 0) return [];
        return tags.Select(NormalizeTag)
            .Where(t => !string.IsNullOrWhiteSpace(t) && !KnownTags.Contains(t))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static ReviewRecord? BuildReviewRecord(string imagePath, string txtPath, List<TagCandidate> rawCandidates, SelectionResult selection, AppOptions options, List<string> unknownFinalTags, bool shortCaption) {
        var rawTokens = rawCandidates.Select(c => NormalizeTag(c.Tag)).Where(t => !string.IsNullOrWhiteSpace(t)).ToList();
        var bannedDetected = rawTokens.Where(t => BannedFinalTags.Contains(t)).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(t => t, StringComparer.OrdinalIgnoreCase).ToList();
        var subjectTokens = rawTokens
            .Where(t => SubjectTokens.Contains(t) || t.Contains("boys", StringComparison.OrdinalIgnoreCase) || t.Contains("girls", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var reasons = new List<string>();
        bool hasMale = bannedDetected.Count > 0 || subjectTokens.Any(t => MaleSubjectTokens.Contains(t));
        bool hasGirl = subjectTokens.Any(t => t.Contains("girl", StringComparison.OrdinalIgnoreCase));
        bool hasMultiPerson = subjectTokens.Any(t => t.StartsWith("multiple_", StringComparison.OrdinalIgnoreCase) || Regex.IsMatch(t, @"^\d+(girl|girls|boy|boys)$", RegexOptions.IgnoreCase));

        if (hasMale) reasons.Add("male_subject_token");
        if (hasMale && hasGirl) reasons.Add("mixed_subject_tokens");
        if (hasMultiPerson) reasons.Add("multi_person");
        if (shortCaption) reasons.Add("short_caption");

        if (reasons.Count == 0 && bannedDetected.Count == 0 && unknownFinalTags.Count == 0) return null;

        var thresholds = options.JoyThresholdSecondary.HasValue
            ? $"primary={options.JoyThreshold:0.00};secondary={options.JoyThresholdSecondary.Value:0.00}"
            : $"primary={options.JoyThreshold:0.00}";

        return new ReviewRecord {
            ImagePath = imagePath,
            CaptionPath = txtPath,
            Reasons = reasons.Distinct(StringComparer.OrdinalIgnoreCase).ToList(),
            BannedTokensDetected = bannedDetected,
            DroppedTags = selection.DroppedMaleSubjectTags,
            FinalTagCount = selection.Tags.Count,
            ShortCaption = shortCaption,
            ThresholdsUsed = thresholds,
            TopJoyTags = rawCandidates.OrderByDescending(c => c.Score).ThenBy(c => c.Order).Take(20).Select(c => c.Tag).ToList(),
            UnknownFinalTags = unknownFinalTags,
            KnownTagDictionaryFound = KnownTagDictionaryFound,
            KnownTagDictionaryCount = KnownTags.Count,
            KnownTagDictionaryPath = KnownTagDictionaryPath ?? ""
        };
    }

    // Review logging is append-only so generation never blocks; manual cleanup happens later.
    private static void AppendReviewLog(string datasetRoot, ReviewRecord record) {
        var jsonlPath = Path.Combine(datasetRoot, "review_candidates.jsonl");
        var csvPath = Path.Combine(datasetRoot, "review_candidates.csv");
        var jsonLine = JsonSerializer.Serialize(record);
        var csvLine = string.Join(",", [
            Csv(record.ImagePath), Csv(record.CaptionPath), Csv(string.Join(";", record.Reasons)), Csv(string.Join(";", record.BannedTokensDetected)),
            Csv(string.Join(";", record.DroppedTags)), Csv(record.FinalTagCount.ToString(CultureInfo.InvariantCulture)), Csv(record.ShortCaption ? "true" : "false"),
            Csv(record.ThresholdsUsed), Csv(string.Join(";", record.TopJoyTags)), Csv(string.Join(";", record.UnknownFinalTags)),
            Csv(record.KnownTagDictionaryFound ? "true" : "false"), Csv(record.KnownTagDictionaryCount.ToString(CultureInfo.InvariantCulture)), Csv(record.KnownTagDictionaryPath)
        ]);

        lock (ReviewLogLock) {
            File.AppendAllText(jsonlPath, jsonLine + Environment.NewLine, new UTF8Encoding(false));
            if (!File.Exists(csvPath) || new FileInfo(csvPath).Length == 0) {
                File.AppendAllText(csvPath, "image_path,caption_path,reasons,banned_tokens_detected,dropped_tags,final_tag_count,short_caption,thresholds_used,top_joy_tags,unknown_final_tags,known_dict_found,known_dict_count,known_dict_path" + Environment.NewLine, new UTF8Encoding(false));
            }
            File.AppendAllText(csvPath, csvLine + Environment.NewLine, new UTF8Encoding(false));
        }
    }

    private static void TryCopyReviewArtifacts(string reviewCopyDir, string imagePath, string txtPath) {
        try {
            Directory.CreateDirectory(reviewCopyDir);
            var name = Path.GetFileName(imagePath);
            var txtName = Path.GetFileName(txtPath);
            File.Copy(imagePath, Path.Combine(reviewCopyDir, name), overwrite: true);
            if (File.Exists(txtPath)) File.Copy(txtPath, Path.Combine(reviewCopyDir, txtName), overwrite: true);
        } catch { }
    }


    private static void WarnDeprecatedFlags(string[] args) {
        var deprecated = new[] { "--min-tags", "--max-tags", "--target-tags", "--max-quality-tags" };
        foreach (var flag in deprecated) {
            if (args.Contains(flag)) {
                Console.WriteLine($"[warn] {flag} はこのバージョンでは廃止されました。無視して続行します。");
            }
        }
    }

    private static ProfileConfig? LoadProfile(string profileName, AppOptions options) {
        var file = Path.Combine(AppContext.BaseDirectory, "profiles", $"{profileName}.json");
        if (!File.Exists(file)) return null;
        try {
            var json = File.ReadAllText(file, Encoding.UTF8);
            var profile = JsonSerializer.Deserialize<ProfileConfig>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            if (profile is null) return null;
            if (!string.IsNullOrWhiteSpace(profile.Trigger)) options.Trigger = NormalizeTag(profile.Trigger);
            profile.AlwaysAdd = profile.AlwaysAdd.Select(NormalizeTag).Where(t => !string.IsNullOrWhiteSpace(t)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            return profile;
        } catch {
            return null;
        }
    }

    private static void BuildForceAllowTags(AppOptions options, ProfileConfig? profile) {
        ForceAllowTags.Clear();
        ForceAllowTags.Add(NormalizeTag(options.Trigger));
        ForceAllowTags.Add("1girl");
        if (profile is not null) {
            foreach (var t in profile.AlwaysAdd) ForceAllowTags.Add(NormalizeTag(t));
        }
    }

    private static string? DetectSafeModeRejectReason(IEnumerable<string> tags) {
        var hit = tags.FirstOrDefault(t => SafeModeRejectTags.Contains(t));
        return hit is null ? null : $"safe_mode:{hit}";
    }

    private static void HandleReject(string imagePath, string txtPath, AppOptions options, string reason) {
        var rejectDir = string.IsNullOrWhiteSpace(options.ReviewRejectDir) ? Path.Combine(options.Dir, "__reject") : options.ReviewRejectDir;
        Directory.CreateDirectory(rejectDir);
        var imageName = Path.GetFileName(imagePath);
        var txtName = Path.GetFileName(txtPath);
        File.Copy(imagePath, Path.Combine(rejectDir, imageName), true);
        if (File.Exists(txtPath)) File.Copy(txtPath, Path.Combine(rejectDir, txtName), true);
        var rejectTextPath = Path.Combine(rejectDir, Path.GetFileNameWithoutExtension(imagePath) + ".reject.txt");
        File.WriteAllText(rejectTextPath, reason + Environment.NewLine, new UTF8Encoding(false));
    }

    private static void AppendAudit(string datasetRoot, string filename, List<string> keptTags, List<string> droppedTagsWithReason, string? rejectReason, string? vocabFile, string usedProfile) {
        var reviewDir = Path.Combine(datasetRoot, "__review");
        Directory.CreateDirectory(reviewDir);
        var path = Path.Combine(reviewDir, "audit.csv");
        lock (ReviewLogLock) {
            if (!File.Exists(path) || new FileInfo(path).Length == 0) {
                File.AppendAllText(path, "filename,kept_tags,dropped_tags_with_reason,reject_reason,used_vocab_file,used_profile" + Environment.NewLine, new UTF8Encoding(false));
            }
            var line = string.Join(",", [Csv(filename), Csv(string.Join(";", keptTags)), Csv(string.Join(";", droppedTagsWithReason)), Csv(rejectReason ?? ""), Csv(vocabFile ?? ""), Csv(usedProfile)]);
            File.AppendAllText(path, line + Environment.NewLine, new UTF8Encoding(false));
        }
    }

    private static string Csv(string s) {
        var escaped = s.Replace(""", """");
        return $""{escaped}"";
    }

    private static void LoadKnownTagDictionary(AppOptions options) {
        KnownTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        KnownTagDictionaryFound = false;
        KnownTagDictionaryPath = null;

        var candidates = new List<string>();
        if (!string.IsNullOrWhiteSpace(options.Dir)) candidates.Add(Path.Combine(options.Dir, "top_tags.txt"));
        candidates.Add(Path.Combine(Directory.GetCurrentDirectory(), "top_tags.txt"));
        candidates.Add(Path.Combine(AppContext.BaseDirectory, "top_tags.txt"));

        foreach (var file in candidates.Distinct(StringComparer.OrdinalIgnoreCase)) {
            if (!File.Exists(file)) continue;
            if (TryLoadKnownTagsFromFile(file, out var loaded) && loaded.Count > 0) {
                KnownTags = loaded;
                KnownTagDictionaryFound = true;
                KnownTagDictionaryPath = file;
                if (!options.StrictKnownTagsExplicit) {
                    options.StrictKnownTags = true;
                }
                ActiveStrictKnownTags = options.StrictKnownTags;
                return;
            }
        }

        if (!options.StrictKnownTagsExplicit) {
            options.StrictKnownTags = false;
        }
        ActiveStrictKnownTags = options.StrictKnownTags;
    }

    private static bool TryLoadKnownTagsFromFile(string path, out HashSet<string> tags) {
        tags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try {
            var ext = Path.GetExtension(path).ToLowerInvariant();
            if (ext == ".json") {
                using var doc = JsonDocument.Parse(File.ReadAllText(path, Encoding.UTF8));
                ExtractKnownTagsFromJson(doc.RootElement, tags);
            } else {
                foreach (var line in File.ReadLines(path, Encoding.UTF8)) {
                    var cleaned = line.Trim();
                    if (string.IsNullOrWhiteSpace(cleaned)) continue;
                    foreach (var raw in cleaned.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
                        var n = NormalizeTag(raw);
                        if (!string.IsNullOrWhiteSpace(n)) tags.Add(n);
                    }
                }
            }
            return tags.Count > 0;
        } catch {
            return false;
        }
    }

    private static void ExtractKnownTagsFromJson(JsonElement element, HashSet<string> tags) {
        switch (element.ValueKind) {
            case JsonValueKind.String:
                var n = NormalizeTag(element.GetString() ?? "");
                if (!string.IsNullOrWhiteSpace(n)) tags.Add(n);
                break;
            case JsonValueKind.Array:
                foreach (var child in element.EnumerateArray()) ExtractKnownTagsFromJson(child, tags);
                break;
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject()) {
                    ExtractKnownTagsFromJson(prop.Value, tags);
                    var pn = NormalizeTag(prop.Name);
                    if (!string.IsNullOrWhiteSpace(pn)) tags.Add(pn);
                }
                break;
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
        if (ForceAllowTags.Contains(lower)) return false;
        if (!IsValidTag(lower)) return true;
        if (DropTagsContains.Any(lower.Contains)) return true;
        if (lower.StartsWith("rating:")) return true;
        if (BuiltinDenyTags.Contains(lower)) return true;
        if (BannedFinalTags.Contains(lower)) return true;
        if (DenylistTags.Contains(lower)) return true;
        if (AllowlistTags.Count > 0 && !AllowlistTags.Contains(lower) && (lower.StartsWith("rating") || lower.Contains("watermark"))) return true;
        if (KnownTagDictionaryFound && ActiveStrictKnownTags && !KnownTags.Contains(lower)) return true;
        return false;
    }

    private static bool IsValidTag(string tag) {
        if (string.IsNullOrWhiteSpace(tag)) return false;
        if (tag == ":" || tag.StartsWith(":", StringComparison.Ordinal)) return false;
        return ValidTagRegex.IsMatch(tag);
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

    private static AppOptions? ParseOptions(string[] args, bool useLast) {
        var saved = LoadWizardConfig();
        WarnDeprecatedFlags(args);
        var dirArg = GetArg(args, "--dir");
        var positionalDir = args.FirstOrDefault(a => !a.StartsWith("-", StringComparison.Ordinal));
        var resolvedDir = dirArg ?? positionalDir;

        if (useLast) {
            var fromSaved = BuildOptionsFromWizardConfig(saved);
            if (!string.IsNullOrWhiteSpace(resolvedDir)) fromSaved.Dir = resolvedDir;
            if (string.IsNullOrWhiteSpace(fromSaved.Dir)) {
                Console.Error.WriteLine("No directory resolved for --use-last. Set it in wizard first or pass --dir <path>.");
                return null;
            }
            if (!Directory.Exists(fromSaved.Dir)) { Console.Error.WriteLine($"Directory not found: {fromSaved.Dir}"); return null; }

            fromSaved.Recursive = args.Contains("--recursive") ? true : fromSaved.Recursive;
            fromSaved.Overwrite = args.Contains("--overwrite") ? true : fromSaved.Overwrite;
            fromSaved.Quiet = args.Contains("--quiet") ? true : fromSaved.Quiet;
            fromSaved.DisableOpenAi = args.Contains("--disable-openai") || args.Contains("--no-openai") || fromSaved.DisableOpenAi;
            fromSaved.ReviewCopyDir = GetArg(args, "--review-copy-dir") ?? fromSaved.ReviewCopyDir;
            fromSaved.StrictKnownTagsExplicit = args.Contains("--strict-known-tags") || args.Contains("--no-strict-known-tags");
            fromSaved.StrictKnownTags = args.Contains("--strict-known-tags") ? true : (args.Contains("--no-strict-known-tags") ? false : fromSaved.StrictKnownTags);
            fromSaved.SafeMode = args.Contains("--no-safe-mode") ? false : fromSaved.SafeMode;
            fromSaved.ReviewRejectDir = GetArg(args, "--review-reject-dir") ?? Path.Combine(fromSaved.Dir, "__reject");
            fromSaved.Trigger = GetArg(args, "--trigger") ?? fromSaved.Trigger;
            fromSaved.Profile = GetArg(args, "--profile") ?? fromSaved.Profile;
            fromSaved.NoProfile = args.Contains("--no-profile") ? true : fromSaved.NoProfile;

            var apiKeySaved = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
            if (fromSaved.DisableOpenAi) apiKeySaved = null;
            fromSaved.ApiKey = apiKeySaved;
            if (!HasUserDefinedMinimumTags(fromSaved)) return null;
            return fromSaved;
        }

        if (string.IsNullOrWhiteSpace(positionalDir)) {
            Console.Error.WriteLine("Directory not found: (missing positional dir). Use --use-last or pass a directory.");
            return null;
        }
        var dir = positionalDir;
        if (!Directory.Exists(dir)) { Console.Error.WriteLine($"Directory not found: {dir}"); return null; }

        var options = new AppOptions {
            Dir = dir,
            Recursive = args.Contains("--recursive"),
            Overwrite = args.Contains("--overwrite"),
            Quiet = args.Contains("--quiet"),
            KeepNeedtag = args.Contains("--keep-needtag"),
            Concurrency = Clamp(ParseInt(GetArg(args, "--concurrency"), 8), 1, 64),
            TargetBytes = Clamp(ParseInt(GetArg(args, "--target-bytes"), 3_000_000), 400_000, 20_000_000),
            JoyUrl = GetArg(args, "--joy-url") ?? "http://127.0.0.1:7865",
            JoyThreshold = ClampDouble(ParseDouble(GetArg(args, "--joy-threshold"), 0.45), 0.10, 0.95),
            JoyThresholdSecondary = ParseOptionalDouble(GetArg(args, "--joy-threshold-secondary"), 0.35),
            OpenAiModel = GetArg(args, "--model") ?? "gpt-4.1-mini",
            OpenAiRetries = Clamp(ParseInt(GetArg(args, "--openai-retries"), 1), 0, 6),
            ReprocessBadExisting = !args.Contains("--no-reprocess-bad-existing"),
            AutoPrefixTags = GetArg(args, "--auto-prefix-tags") ?? "",
            MinimalSubjectTags = GetArg(args, "--minimal-subject-tags") ?? "",
            AutoStartJoyTag = args.Contains("--auto-start-joytag"),
            JoyTagPythonExe = GetArg(args, "--joytag-python"),
            JoyTagWorkingDir = GetArg(args, "--joytag-dir"),
            JoyAutoPort = args.Contains("--joy-auto-port"),
            JoyKillConflictProcess = args.Contains("--joy-kill-conflict"),
            DisableOpenAi = args.Contains("--disable-openai") || args.Contains("--no-openai"),
            ReviewCopyDir = GetArg(args, "--review-copy-dir"),
            StrictKnownTagsExplicit = args.Contains("--strict-known-tags") || args.Contains("--no-strict-known-tags"),
            StrictKnownTags = args.Contains("--strict-known-tags"),
            Trigger = GetArg(args, "--trigger") ?? "sgrui",
            Profile = GetArg(args, "--profile") ?? "sgrui",
            NoProfile = args.Contains("--no-profile"),
            SafeMode = args.Contains("--no-safe-mode") ? false : true,
            ReviewRejectDir = GetArg(args, "--review-reject-dir") ?? Path.Combine(dir, "__reject")
        };

        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        if (options.DisableOpenAi) apiKey = null;
        options.ApiKey = apiKey;

        if (!HasUserDefinedMinimumTags(options)) return null;
        return options;
    }

    private static AppOptions BuildOptionsFromWizardConfig(WizardConfig saved) {
        return new AppOptions {
            Dir = saved.LastDir,
            Recursive = saved.Recursive,
            Overwrite = saved.Overwrite,
            Quiet = saved.Quiet,
            KeepNeedtag = saved.KeepNeedtag,
            Concurrency = saved.Concurrency,
            TargetBytes = saved.TargetBytes,
            JoyUrl = saved.JoyUrl,
            JoyThreshold = saved.JoyThreshold,
            JoyThresholdSecondary = saved.JoyThresholdSecondary,
            OpenAiModel = saved.OpenAiModel,
            OpenAiRetries = saved.OpenAiRetries,
            DisableOpenAi = saved.DisableOpenAi,
            ReprocessBadExisting = true,
            AutoPrefixTags = saved.AutoPrefixTags,
            MinimalSubjectTags = saved.MinimalSubjectTags,
            AutoStartJoyTag = saved.AutoStartJoyTag,
            JoyTagWorkingDir = saved.JoyTagWorkingDir,
            JoyTagPythonExe = saved.JoyTagPythonExe,
            JoyAutoPort = saved.JoyAutoPort,
            JoyKillConflictProcess = saved.JoyKillConflictProcess,
            ReviewCopyDir = saved.ReviewCopyDir,
            StrictKnownTags = saved.StrictKnownTags,
            StrictKnownTagsExplicit = false,
            Trigger = saved.Trigger,
            Profile = saved.Profile,
            NoProfile = saved.NoProfile,
            SafeMode = saved.SafeMode,
            ReviewRejectDir = saved.ReviewRejectDir
        };
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
            TargetBytes = PromptInt("送信用ターゲットサイズ(bytes)", saved.TargetBytes, 400_000, 20_000_000),
            JoyUrl = PromptString("JoyTag URL", saved.JoyUrl),
            JoyThreshold = PromptDouble("JoyTag threshold", saved.JoyThreshold, 0.10, 0.95),
            JoyThresholdSecondary = PromptOptionalDouble("JoyTag secondary threshold(空で無効)", saved.JoyThresholdSecondary, 0.10, 0.95),
            OpenAiModel = saved.OpenAiModel,
            OpenAiRetries = saved.OpenAiRetries,
            DisableOpenAi = PromptBoolByEmpty("OpenAI無効化？", saved.DisableOpenAi),
            AutoPrefixTags = PromptString("常に先頭へ付けるタグ", saved.AutoPrefixTags),
            MinimalSubjectTags = PromptString("失敗時フォールバックタグ", saved.MinimalSubjectTags),
            AutoStartJoyTag = PromptBoolByEmpty("JoyTag自動起動？", saved.AutoStartJoyTag),
            JoyTagWorkingDir = PromptString("JoyTagフォルダ", saved.JoyTagWorkingDir),
            JoyTagPythonExe = PromptString("JoyTag python.exe", saved.JoyTagPythonExe),
            JoyAutoPort = PromptBoolByEmpty("ポート競合時に自動で別ポートを使う？", saved.JoyAutoPort),
            JoyKillConflictProcess = PromptBoolByEmpty("ポート競合プロセスを自動終了する？", saved.JoyKillConflictProcess),
            ReviewCopyDir = PromptString("レビュー用コピー先フォルダ(空で無効)", saved.ReviewCopyDir),
            StrictKnownTags = PromptBoolByEmpty("未知タグを除外する？(--strict-known-tags)", saved.StrictKnownTags),
            StrictKnownTagsExplicit = false,
            Trigger = PromptString("trigger token", saved.Trigger),
            Profile = PromptString("profile名", saved.Profile),
            NoProfile = PromptBoolByEmpty("プロファイル強制付与を無効化？(--no-profile)", saved.NoProfile),
            SafeMode = PromptBoolByEmpty("Safe Mode有効？(--safe-mode)", saved.SafeMode),
            ReviewRejectDir = PromptString("reject出力先", string.IsNullOrWhiteSpace(saved.ReviewRejectDir) ? Path.Combine(dir, "__reject") : saved.ReviewRejectDir)
        };


        var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        if (options.DisableOpenAi) apiKey = null;
        options.ApiKey = apiKey;

        if (!HasUserDefinedMinimumTags(options)) return null;

        SaveWizardConfig(ToWizardConfig(options));

        return options;
    }

    private static bool HasUserDefinedMinimumTags(AppOptions options) {
        var hasAny = SplitTags(options.AutoPrefixTags).Any() || SplitTags(options.MinimalSubjectTags).Any();
        if (hasAny) return true;
        Console.Error.WriteLine("最低限タグが未設定です。--auto-prefix-tags または --minimal-subject-tags で指定してください。");
        return false;
    }

    private static async Task<(int ExitCode, Process? AutoStartedProcess)> StartOrReuseJoyTagServerAsync(AppOptions options) {
        if ((await CheckJoyTagHealthAsync(options.JoyUrl, CancellationToken.None)).IsAlive) {
            Console.WriteLine("JoyTag is already alive. Reusing existing server.");
            return (0, null);
        }
        if (string.IsNullOrWhiteSpace(options.JoyTagPythonExe) || string.IsNullOrWhiteSpace(options.JoyTagWorkingDir)) {
            Console.Error.WriteLine("[error] --auto-start-joytag requires --joytag-python and --joytag-dir.");
            return (2, null);
        }

        if (TryGetLocalPort(options.JoyUrl, out var requestedPort) && !IsPortAvailable(requestedPort)) {
            var currentPort = requestedPort;
            var conflictingPids = GetListeningProcessIds(currentPort).ToArray();
            if (options.JoyKillConflictProcess && conflictingPids.Length > 0) {
                Console.WriteLine($"[info] JoyTag URL port {currentPort} is occupied. Trying to kill listener processes: {string.Join(", ", conflictingPids)}");
                KillProcesses(conflictingPids);
                await Task.Delay(800);
            }

            if (!IsPortAvailable(currentPort) && options.JoyAutoPort && TryFindAvailablePort(currentPort + 1, out var fallbackPort) && TryReplaceUrlPort(options.JoyUrl, fallbackPort, out var changedUrl)) {
                Console.WriteLine($"[info] JoyTag URL port {currentPort} is occupied. Switched to {changedUrl} (--joy-auto-port).");
                options.JoyUrl = changedUrl;
                currentPort = fallbackPort;
            }

            if (!IsPortAvailable(currentPort)) {
                Console.Error.WriteLine($"[error] JoyTag URL port {currentPort} is already in use, but JoyTag health check failed.");
                if (conflictingPids.Length > 0) {
                    Console.Error.WriteLine($"        listener pid: {string.Join(", ", conflictingPids)}");
                }
                Console.Error.WriteLine("        --joy-auto-port を有効化して自動ポート変更するか、--joy-kill-conflict で競合プロセスを自動終了してください。");
                return (2, null);
            }
        }

        if (!TryGetLocalPort(options.JoyUrl, out var joyPort)) {
            Console.Error.WriteLine($"[error] Failed to parse loopback port from JoyUrl: {options.JoyUrl}");
            return (2, null);
        }

        Console.WriteLine($"[info] Starting JoyTag with uvicorn on port {joyPort}");

        var psi = new ProcessStartInfo {
            FileName = options.JoyTagPythonExe,
            WorkingDirectory = options.JoyTagWorkingDir,
            Arguments = $"-m uvicorn joytag_server:app --host 127.0.0.1 --port {joyPort} --workers 1",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        var process = Process.Start(psi);
        if (process is null) return (2, null);

        var stdoutLog = new ConcurrentQueue<string>();
        var stderrLog = new ConcurrentQueue<string>();
        process.OutputDataReceived += (_, e) => { if (!string.IsNullOrWhiteSpace(e.Data)) EnqueueLog(stdoutLog, e.Data); };
        process.ErrorDataReceived += (_, e) => { if (!string.IsNullOrWhiteSpace(e.Data)) EnqueueLog(stderrLog, e.Data); };
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        var startupTimeout = TimeSpan.FromSeconds(180);
        var startupWatch = Stopwatch.StartNew();
        var isDetachedWait = false;
        var nextDetachedStatus = TimeSpan.Zero;
        var nextHealthDiagnostic = TimeSpan.Zero;
        var lastHealthDiagnosticKey = string.Empty;

        while (startupWatch.Elapsed < startupTimeout) {
            var elapsedSeconds = (int)startupWatch.Elapsed.TotalSeconds;
            var isPortListening = TryGetLocalPort(options.JoyUrl, out var joyPortForListen) && IsPortListening(joyPortForListen);
            var health = await CheckJoyTagHealthAsync(options.JoyUrl, CancellationToken.None);

            if (health.IsAlive) {
                if (isDetachedWait) {
                    Console.WriteLine("[info] JoyTag became reachable after launcher exit. Continuing with existing server.");
                    return (0, null);
                }
                return (0, process);
            }

            if (startupWatch.Elapsed >= nextHealthDiagnostic) {
                if (isPortListening && health.HadTimeout) {
                    Console.WriteLine("[info] Port is listening but HTTP is slow; increasing wait.");
                }
                if (health.LastException is not null) {
                    var key = health.LastException.GetType().Name + ":" + health.LastException.Message;
                    if (!string.Equals(lastHealthDiagnosticKey, key, StringComparison.Ordinal)) {
                        Console.WriteLine($"[info] JoyTag health check pending: {health.LastException.GetType().Name}: {health.LastException.Message}");
                        lastHealthDiagnosticKey = key;
                    }
                }
                nextHealthDiagnostic = startupWatch.Elapsed + TimeSpan.FromSeconds(10);
            }

            if (process is not null && process.HasExited) {
                var exitCode = process.ExitCode;
                if (exitCode == 0) {
                    Console.WriteLine("[info] JoyTag launcher process exited with code 0. Waiting for detached server startup...");
                    process = null;
                    isDetachedWait = true;
                    nextDetachedStatus = TimeSpan.FromSeconds(Math.Max(5, elapsedSeconds + 5));
                } else {
                    Console.Error.WriteLine($"[error] JoyTag process exited before ready. exit={exitCode}");
                    if (isPortListening) {
                        Console.Error.WriteLine("[info] JoyTag URL port is LISTENING; continuing to wait for HTTP readiness.");
                        process = null;
                        isDetachedWait = true;
                        nextDetachedStatus = TimeSpan.FromSeconds(Math.Max(5, elapsedSeconds + 5));
                    } else {
                        PrintRecentJoyTagLogs(stdoutLog, stderrLog);
                        return (2, null);
                    }
                }
            }

            if (isDetachedWait && startupWatch.Elapsed >= nextDetachedStatus) {
                Console.WriteLine($"[info] Waiting for JoyTag server to become reachable... (elapsed {elapsedSeconds}s)");
                nextDetachedStatus += TimeSpan.FromSeconds(5);
            }

            await Task.Delay(1000);
        }

        var timedOutPortListening = TryGetLocalPort(options.JoyUrl, out var timeoutPort) && IsPortListening(timeoutPort);
        Console.Error.WriteLine("[error] JoyTag did not become ready in time.");
        if (timedOutPortListening) {
            Console.Error.WriteLine("[info] JoyTag URL port is LISTENING, but HTTP health check still fails. Check JoyTag bind host and local firewall settings.");
        }
        PrintRecentJoyTagLogs(stdoutLog, stderrLog);
        return (2, null);
    }

    private static void EnqueueLog(ConcurrentQueue<string> queue, string line) {
        queue.Enqueue(line);
        while (queue.Count > 40) queue.TryDequeue(out _);
    }

    private static void PrintRecentJoyTagLogs(ConcurrentQueue<string> stdoutLog, ConcurrentQueue<string> stderrLog) {
        if (!stdoutLog.IsEmpty) {
            Console.Error.WriteLine("[info] JoyTag stdout (recent):");
            foreach (var line in stdoutLog) Console.Error.WriteLine("  " + line);
        }
        if (!stderrLog.IsEmpty) {
            Console.Error.WriteLine("[info] JoyTag stderr (recent):");
            foreach (var line in stderrLog) Console.Error.WriteLine("  " + line);
        }
    }

    private static bool TryGetLocalPort(string baseUrl, out int port) {
        port = 0;
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var uri)) return false;
        if (!uri.IsLoopback) return false;
        if (uri.Port is <= 0 or > 65535) return false;
        port = uri.Port;
        return true;
    }

    private static bool IsPortAvailable(int port) {
        TcpListener? listener = null;
        try {
            listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            return true;
        } catch (SocketException) {
            return false;
        } finally {
            try { listener?.Stop(); } catch { }
        }
    }

    private static bool IsPortListening(int port) => GetListeningProcessIds(port).Any();

    private static bool TryFindAvailablePort(int startPort, out int foundPort) {
        for (int p = Math.Max(1024, startPort); p <= 65535; p++) {
            if (IsPortAvailable(p)) {
                foundPort = p;
                return true;
            }
        }
        foundPort = 0;
        return false;
    }

    private static bool TryReplaceUrlPort(string baseUrl, int newPort, out string replaced) {
        replaced = baseUrl;
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var uri)) return false;
        var builder = new UriBuilder(uri) { Port = newPort };
        replaced = builder.Uri.ToString().TrimEnd('/');
        return true;
    }

    private static IEnumerable<int> GetListeningProcessIds(int port) {
        var pids = new HashSet<int>();
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
            foreach (var line in RunCommandForOutput("netstat", "-ano -p tcp")) {
                var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 5 || !parts[1].EndsWith($":{port}", StringComparison.Ordinal) || !parts[3].Equals("LISTENING", StringComparison.OrdinalIgnoreCase)) continue;
                if (int.TryParse(parts[4], out var pid) && pid > 0) pids.Add(pid);
            }
            return pids;
        }

        foreach (var line in RunCommandForOutput("lsof", $"-nP -iTCP:{port} -sTCP:LISTEN -t")) {
            if (int.TryParse(line.Trim(), out var pid) && pid > 0) pids.Add(pid);
        }
        if (pids.Count > 0) return pids;

        foreach (var line in RunCommandForOutput("ss", $"-lptn sport = :{port}")) {
            var m = Regex.Match(line, @"pid=(\d+)");
            if (m.Success && int.TryParse(m.Groups[1].Value, out var pid) && pid > 0) pids.Add(pid);
        }
        return pids;
    }

    private static IEnumerable<string> RunCommandForOutput(string fileName, string arguments) {
        try {
            var psi = new ProcessStartInfo {
                FileName = fileName,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };
            using var process = Process.Start(psi);
            if (process is null) return [];
            var output = process.StandardOutput.ReadToEnd();
            process.WaitForExit(3000);
            return output.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        } catch {
            return [];
        }
    }

    private static void KillProcesses(IEnumerable<int> pids) {
        foreach (var pid in pids.Distinct()) {
            try {
                using var process = Process.GetProcessById(pid);
                if (!process.HasExited) process.Kill(entireProcessTree: true);
            } catch { }
        }
    }

    private sealed class JoyTagHealthResult {
        public bool IsAlive { get; init; }
        public bool HadTimeout { get; init; }
        public Exception? LastException { get; init; }
    }

    private static async Task<JoyTagHealthResult> CheckJoyTagHealthAsync(string baseUrl, CancellationToken ct) {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        var hadTimeout = false;
        Exception? lastException = null;

        try {
            using var healthResp = await http.GetAsync(baseUrl.TrimEnd('/') + "/health", ct);
            if (healthResp.IsSuccessStatusCode) {
                var healthJson = await healthResp.Content.ReadAsStringAsync(ct);
                using var healthDoc = JsonDocument.Parse(healthJson);
                if (healthDoc.RootElement.TryGetProperty("ok", out var okEl) && okEl.ValueKind == JsonValueKind.True) {
                    return new JoyTagHealthResult { IsAlive = true };
                }
            }
        } catch (OperationCanceledException ex) when (!ct.IsCancellationRequested) {
            hadTimeout = true;
            lastException = ex;
        } catch (Exception ex) {
            lastException = ex;
        }

        var endpoints = new[] { "/", "/docs", "/openapi.json" };
        foreach (var endpoint in endpoints) {
            try {
                using var resp = await http.GetAsync(baseUrl.TrimEnd('/') + endpoint, ct);
                if ((int)resp.StatusCode < 500) {
                    return new JoyTagHealthResult { IsAlive = true };
                }
            } catch (OperationCanceledException ex) when (!ct.IsCancellationRequested) {
                hadTimeout = true;
                lastException = ex;
            } catch (Exception ex) {
                lastException = ex;
            }
        }

        return new JoyTagHealthResult {
            IsAlive = false,
            HadTimeout = hadTimeout,
            LastException = lastException
        };
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
            Trigger = "trigger_token",
            MinimalSubjectTags = "1girl"
        };
        BuildForceAllowTags(options, null);

        var candidates = ParseTagCandidatesFromLine("1girl, solo, blue_hair, long_hair, blue_eyes, smile, school_uniform, standing, classroom, masterpiece, best_quality, highres, watermark", null, 1.0);
        var tags = SelectTags(candidates, options).Tags;

        bool prefixFirst = tags.Count > 0 && tags[0] == "trigger_token";
        bool hasSubjectEarly = tags.Take(3).Contains("1girl");
        bool maxQualityRespected = tags.Count(t => ClassifyTag(t) == TagBucket.StyleQuality) <= MaxStyleTagCap;
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

    private static WizardConfig ToWizardConfig(AppOptions options) {
        return new WizardConfig {
            LastDir = options.Dir,
            JoyUrl = options.JoyUrl,
            Concurrency = options.Concurrency,
            TargetBytes = options.TargetBytes,
            Recursive = options.Recursive,
            Overwrite = options.Overwrite,
            Quiet = options.Quiet,
            KeepNeedtag = options.KeepNeedtag,
            AutoPrefixTags = options.AutoPrefixTags,
            MinimalSubjectTags = options.MinimalSubjectTags,
            AutoStartJoyTag = options.AutoStartJoyTag,
            JoyTagWorkingDir = options.JoyTagWorkingDir ?? "",
            JoyTagPythonExe = options.JoyTagPythonExe ?? "",
            JoyAutoPort = options.JoyAutoPort,
            JoyKillConflictProcess = options.JoyKillConflictProcess,
            JoyThreshold = options.JoyThreshold,
            JoyThresholdSecondary = options.JoyThresholdSecondary,
            OpenAiModel = options.OpenAiModel,
            OpenAiRetries = options.OpenAiRetries,
            DisableOpenAi = options.DisableOpenAi,
            ReviewCopyDir = options.ReviewCopyDir ?? "",
            StrictKnownTags = options.StrictKnownTags,
            Trigger = options.Trigger,
            Profile = options.Profile,
            NoProfile = options.NoProfile,
            SafeMode = options.SafeMode,
            ReviewRejectDir = options.ReviewRejectDir
        };
    }

    private sealed class AppOptions {
        public string Dir { get; set; } = "";
        public bool Recursive { get; set; } = true;
        public bool Overwrite { get; set; }
        public bool Quiet { get; set; } = true;
        public bool KeepNeedtag { get; set; } = true;
        public int Concurrency { get; set; } = 12;
        public int TargetBytes { get; set; } = 3_000_000;
        public string JoyUrl { get; set; } = "http://127.0.0.1:7865";
        public double JoyThreshold { get; set; } = 0.45;
        public double? JoyThresholdSecondary { get; set; } = 0.35;
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
        public bool JoyAutoPort { get; set; } = true;
        public bool JoyKillConflictProcess { get; set; }
        public string? ReviewCopyDir { get; set; }
        public bool StrictKnownTags { get; set; }
        public bool StrictKnownTagsExplicit { get; set; }
        public string Trigger { get; set; } = "sgrui";
        public string Profile { get; set; } = "sgrui";
        public bool NoProfile { get; set; }
        public bool SafeMode { get; set; } = true;
        public string ReviewRejectDir { get; set; } = "";
        public int SecondaryNewTagsCap { get; set; } = 15;
    }

    private sealed class WizardConfig {
        public string LastDir { get; set; } = "";
        public string JoyUrl { get; set; } = "http://127.0.0.1:7865";
        public int Concurrency { get; set; } = 12;
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
        public bool JoyAutoPort { get; set; } = true;
        public bool JoyKillConflictProcess { get; set; }
        public double JoyThreshold { get; set; } = 0.45;
        public double? JoyThresholdSecondary { get; set; } = 0.35;
        public string OpenAiModel { get; set; } = "gpt-4.1-mini";
        public int OpenAiRetries { get; set; } = 1;
        public bool DisableOpenAi { get; set; }
        public string ReviewCopyDir { get; set; } = "";
        public bool StrictKnownTags { get; set; }
        public string Trigger { get; set; } = "sgrui";
        public string Profile { get; set; } = "sgrui";
        public bool NoProfile { get; set; }
        public bool SafeMode { get; set; } = true;
        public string ReviewRejectDir { get; set; } = "";
    }

    private static void PrintHelp() {
        Console.WriteLine(
@"ImageCaptioner

Usage:
  ImageCaptioner
  ImageCaptioner <dir> [--recursive] [--overwrite] [--quiet]
    [--concurrency N] [--target-bytes BYTES]
        [--joy-url http://127.0.0.1:7865]
    [--joy-threshold 0.45] [--joy-threshold-secondary 0.35|off]
    [--disable-openai|--no-openai] [--model MODEL] [--openai-retries N]
    [--keep-needtag] [--no-reprocess-bad-existing]
    [--auto-prefix-tags <comma,separated,tags>]
    [--minimal-subject-tags <comma,separated,tags>]
    [--auto-start-joytag --joytag-dir D:\tools\joytag --joytag-python D:\tools\joytag\venv\Scripts\python.exe]
    [--joy-auto-port] [--joy-kill-conflict]
    [--review-copy-dir <dir>] [--strict-known-tags|--no-strict-known-tags]
    [--trigger <token>] [--profile <name>] [--no-profile]
    [--safe-mode|--no-safe-mode] [--review-reject-dir <dir>]
    [--use-last] [--dir <path>]
    [--self-check]

Notes:
  - Caption output is always one line: tag1, tag2, ...
  - Tag selection is bucket-priority with fixed caps and MAX_TOTAL_TAGS=42.
  - JoyTag uses primary + optional secondary pass and merges by normalized tag (higher score wins).
  - OpenAI is optional fallback only when core buckets are missing (non-explicit prompt).
  - allowlist.txt / denylist.txt in working directory are loaded at startup (denylist wins).
  - Final captions always force 1girl and remove banned male-subject tokens (logged for review).
  - top_tags.txt is prioritized as vocab (dir > cwd > app dir). Found top_tags enables strict vocab by default.
  - trigger/profile always-add tags are exempt from strict vocab filtering.
  - Safe Mode is ON by default and rejects risky captions into __reject.");
    }
}
