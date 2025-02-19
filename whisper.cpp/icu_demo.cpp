// g++ -Wall -std=c++17 icu_demo.cpp -I/usr/include -L/usr/lib/x86_64-linux-gnu -licui18n -licuuc -licudata -o icu_demo

#include <algorithm>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include <unicode/regex.h>
#include <unicode/unistr.h>


static std::string normalize_text(const std::string &inp) {
    UErrorCode status = U_ZERO_ERROR;
    const auto replacement = icu::UnicodeString("");
    icu::RegexMatcher matcher(icu::UnicodeString("[^\\p{Letter}\\p{Decimal_Number}\\-\\s]+"), 0, status);

    std::string out;
    icu::UnicodeString text(inp.c_str());
    text.trim().toLower().toUTF8String(out);

    text = matcher.reset(text).replaceAll(replacement, status);
    if (U_SUCCESS(status)) {
        out.clear();
        text.toUTF8String(out);
    }
    return out;
}

static std::vector<std::string> read_vocab(const std::string & fname) {
    std::vector<std::string> vocab;
    std::ifstream ifs(fname);
    if (ifs.is_open()) {
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            vocab.push_back(std::move(normalize_text(line)));
        }
    }
    return vocab;
}

int main(int argc, char ** argv) {
    std::vector<std::string> vocab = read_vocab("commands.txt");
    for (const auto & cmd : vocab) fprintf(stdout, "command: %s\n", cmd.c_str());
    return 0;
}
