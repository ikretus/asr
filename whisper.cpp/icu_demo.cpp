// g++ -Wall -std=c++17 icu.cpp -I/usr/include -L/usr/lib/x86_64-linux-gnu -licui18n -licuuc -licudata -o icu

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

static std::vector<std::string> read_allowed_commands(const std::string & fname) {
    std::vector<std::string> allowed_commands;
    std::string line;

    std::ifstream ifs(fname);
    if (!ifs.is_open()) return allowed_commands;

    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        allowed_commands.push_back(std::move(normalize_text(line)));
    }
    return allowed_commands;
}

int main(int argc, char ** argv) {
    std::vector<std::string> allowed_commands = read_allowed_commands("commands.txt");
    for (const auto & cmd : allowed_commands) fprintf(stdout, "command: %s\n", cmd.c_str());
    return 0;
}
