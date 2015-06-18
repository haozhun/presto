/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.logentries.re2.RE2;
import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Matcher;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Syntax;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 0)
@Measurement(iterations = 1, time = 1000, timeUnit = MILLISECONDS)
public class RegexpLibraryBenchmark
{
    @Benchmark
    public boolean benchmarkJoniPattern(DotStarAroundData data)
    {
        Matcher m = data.joniPattern.matcher(data.sourceBytes);
        int offset = m.search(0, data.sourceBytes.length, Option.DEFAULT);
        boolean result = offset != -1;
        checkState(result == data.found);
        return result;
    }

    @Benchmark
    public boolean benchmarkJavaPattern(DotStarAroundData data)
    {
        boolean result = data.javaPattern.matcher(data.sourceString).find();
        checkState(result == data.found);
        return result;
    }

    @Benchmark
    public boolean benchmarkRe2jPattern(DotStarAroundData data)
    {
        boolean result = data.re2jPattern.matcher(data.sourceString).find();
        checkState(result == data.found);
        return result;
    }

    @Benchmark
    public boolean benchmarkRe2jniPattern(DotStarAroundData data)
    {
        boolean result = data.re2jniPattern.partialMatch(data.sourceString);
        checkState(result == data.found);
        return result;
    }

    @State(Thread)
    public static class DotStarAroundData
    {
        @Param({
                "x", ".*x.*", "^.*x.*$", "a.*a.*i",
                "x|y|z|...", "^(x|y|z|...)$", "[0-9]+", "[^0-9]", "^https?://",
                "extract 0", "extract 1", "extract 2", "extract 3", "extract 3b", "extract a1", "extract a2", "extract a3",
                "complex 1", "complex 2", "complex 3", "complex 3b", "complex 4"
        })
        private String patternName;

        @Param({ "true", "false" })
        private boolean found;

        private Regex joniPattern;
        private Pattern javaPattern;
        private com.google.re2j.Pattern re2jPattern;
        private String sourceString;
        private byte[] sourceBytes;
        private RE2 re2jniPattern;

        @Setup
        public void setup()
        {
            String patternString;
            switch (patternName) {
                case "x":
                    patternString = "IPHONE";
                    sourceString = found ? "PERSONALITY_TAG_APPROVE_ON_IPHONE" : "PERSONALITY_TAG_APPROVE_ON_ANDROID";
                    break;
                case ".*x.*":
                    patternString = ".*IPHONE.*";
                    sourceString = found ? "PERSONALITY_TAG_APPROVE_ON_IPHONE" : "PERSONALITY_TAG_APPROVE_ON_ANDROID";
                    break;
                case "^.*x.*$":
                    patternString = "^.*IPHONE.*$";
                    sourceString = found ? "IPHONE" : "ANDROID";
                    break;
                case "a.*a.*i":
                    checkState(!found, "invalid combination");
                    patternString = "a.*a.*i";
                    sourceString = IntStream.generate(() -> 97).limit(512).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
                    break;
                case "x|y|z|...":
                    patternString = "(adultcontent|adulthealthverybad|badsubscription|cashadvance|casualdating|dating|datingsexual|misuseoflike|hotbutton|language|nonoffer|sexual|shockandscare|wfh|unsubstantiatedclaim|alcohol|badhealthproduct|cloaking|gambling|illegal|onlinepharm|pennyauctions|porn|tobaccosale|usertrust|weaponsale)";
                    sourceString = found ? "usertrust" : "suitable";
                    break;
                case "^(x|y|z|...)$":
                    patternString = "^(adultcontent|adulthealthverybad|badsubscription|cashadvance|casualdating|dating|datingsexual|misuseoflike|hotbutton|language|nonoffer|sexual|shockandscare|wfh|unsubstantiatedclaim|alcohol|badhealthproduct|cloaking|gambling|illegal|onlinepharm|pennyauctions|porn|tobaccosale|usertrust|weaponsale)$";
                    sourceString = found ? "usertrust" : "suitable";
                    break;
                case "[0-9]+":
                    patternString = "[0-9]+";
                    sourceString = found ? "123456789" : "abcdefghijk";
                    break;
                case "[^0-9]":
                    patternString = "[^0-9]";
                    sourceString = found ? "123.456.789" : "123456789";
                    break;
                case "^https?://":
                    patternString = "^https?://";
                    sourceString = found ? "http://www.facebook.com" : "ftp://user:pass@facebook.com";
                    break;
                case "extract 0":
                    checkState(found, "invalid combination");
                    patternString = "_(\\w+)";
                    sourceString = "";
                    break;
                case "extract 1":
                    checkState(found, "invalid combination");
                    patternString = "answer\":\".*\",";
                    sourceString = "{\"response_id\":760724101310,\"answer_index\":1,\"answer\":\"Built sometime in the 1980s\",\"response_location\":\"web\"}";
                    break;
                case "extract 2":
                    checkState(found, "invalid combination");
                    patternString = "(.*?)(Market|market)";
                    sourceString = "Special Market - Trend";
                    break;
                case "extract 3":
                    checkState(found, "invalid combination");
                    patternString = ".*_([a-z]+)_tag";
                    sourceString = "[\"hr_accountillegal_tag\",\"auto_accounttobaccosale_tag\",\"auto_accountweaponsale_tag\"]";
                    break;
                case "extract 3b":
                    checkState(found, "invalid combination");
                    patternString = "^.*(hr_[a-z_]+_tag|auto_[a-z]+_tag).*$";
                    sourceString = "[\"hr_accountillegal_tag\",\"auto_accounttobaccosale_tag\",\"auto_accountweaponsale_tag\"]";
                    break;
                case "extract a1":
                    checkState(found, "invalid combination");
                    patternString = "(?<=PolicyChain:)\\S*";
                    sourceString = "Redirect to blocked url 5:05f7a9d9b3964baac4a9501c47ddef6a xxx.com (PolicyChain:URLsDeletedByReps URL was xxxxx xxxxxx xxx)";
                    break;
                case "extract a2":
                    checkState(found, "invalid combination");
                    patternString = "(?<=PolicyChain:).*?(?=\\s)"; // The regex can be rewritten into one without look-ahead.
                    sourceString = "Redirect to blocked url 5:05f7a9d9b3964baac4a9501c47ddef6a xxx.com (PolicyChain:URLsDeletedByReps URL was xxxxx xxxxxx xxx)";
                    break;
                case "extract a3":
                    checkState(found, "invalid combination");
                    patternString = "^.*(rep_ad_|rep_)((?!ad_)[a-z]+).*$"; // The look-behind is totally unnecessary. The regex is equivalent with / without.
                    sourceString = "[\"rep_ad_flag\",\"rep_ad_suitable\",\"rep_suspected_cloaking\"]";
                    break;
                case "complex 1":
                    patternString = "^(\\w+ )?\\w+(\\.\\w+)+( \\w+)?$";
                    sourceString = found ? "23E9BJ2UMB.caw.largetownship.iphxne" : "##_iPhone_AT&T_8.3_iplug_en_2.0.27_APL000_llpz";
                    break;
                case "complex 2": // extract domain name with www. removed
                    checkState(found, "invalid combination");
                    patternString = "^.*://(?:[wW]{3}\\.)?([^:/]*).*$";
                    sourceString = "https://www.google.com/search?q=test&newwindow=1&biw=1269&bih=909&source=lnms&tbm=isch&sa=X&ei=oBGCVdH5I8AwpASQ_KvoCg&ved=0CBcQ_AVoAg";
                    break;
                case "complex 3": // emoji
                    patternString = "(:\\w+:|<[/\\\\]?3|[()\\\\D|*$][-^]?[:;=]|[:;=B8][-^]?[3DOPp@$*\\\\)(/|]|!{2,})(?=\\s|[!.?]|$)";
                    sourceString = found ? "At Home Wishing I Could Get Xxxxx :)" : "At Home Wishing I Could Get Xxxxx";
                    break;
                case "complex 3b": // emoji
                    patternString = "(:\\w+:|<[/\\\\]?3|[()\\\\D|*$][-^]?[:;=]|[:;=B8][-^]?[3DOPp@$*\\\\)(/|]|!{2,})(?:\\s|[!.?]|$)";
                    sourceString = found ? "At Home Wishing I Could Get Xxxxx :)" : "At Home Wishing I Could Get Xxxxx";
                    break;
                case "complex 4":
                    patternString = "^(\\w{3}\\s{1,2}\\d+\\s\\d+:\\d+:\\d+)\\s([\\w\\W]+?)\\s(bro_.+)\\s(.+?(?=#011))(.+?(?=#011))(.+?(?=#011))(.+?(?=#011))(.+?(?=#011))(.+?(?=#011))";
                    sourceString = found
                            ? "Jun 17 21:05:15 frc-nsmbro04 bro_dns 1434600298.434692#011C8rS3r2blHNw164eLc#011172.17.64.10#01150671#011192.168.12.165#01153#011udp#01125844#011authorizeddns.us#0111#011C_INTERNET#0116#011SOA#011-#011-#011F#011F#011F#011F#0110#011-#011-#011F"
                            : "Jun 16 22:57:48 twi02297.03.ash5.facebook.com sshd[468541]: User child is on pid 468543 session=twi02297.03.ash5:55810c5c.883ae";
                    break;
                default:
                    throw new IllegalStateException();
            }

            sourceBytes = sourceString.getBytes(UTF_8);

            // joni
            joniPattern = buildJoniPattern(patternString);

            // Java pattern
            javaPattern = Pattern.compile(patternString);

            // re2j pattern
            try {
                re2jPattern = com.google.re2j.Pattern.compile(patternString);
            }
            catch (com.google.re2j.PatternSyntaxException ex) {
                re2jPattern = null;
            }

            try {
                re2jniPattern = RE2.compile(patternString);
            }
            catch (IllegalArgumentException ex) {
                re2jniPattern = null;
            }
        }

        private static Regex buildJoniPattern(String patternString)
        {
            byte[] patternBytes = patternString.getBytes();
            Regex regex;
            try {
                // When normal UTF8 encoding instead of non-strict UTF8) is used, joni can infinite loop when invalid UTF8 slice is supplied to it.
                regex = new Regex(patternBytes, 0, patternBytes.length, Option.DEFAULT, NonStrictUTF8Encoding.INSTANCE, Syntax.Java);
            }
            catch (Exception e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
            }
            return regex;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + RegexpLibraryBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
