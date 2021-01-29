#include <iostream>
#include <string_view>

#include "chfiller_tests.hpp"
#include <gflags/gflags.h>
using namespace std;

namespace {
DEFINE_bool(rewrite, false, "if supplied drop the current table");
DEFINE_string(drivers, "", "path to a file containing drivers\' data");
}
/*!
 * @brief uploads data from --drivers file to CH table
 * @param client Clickhouse client
 * @param db_name name of data base to be used
 * @param table_name table to be created and filled
 * @param scheme a vector of string pairs. example:
 *   {{"id", "UInt64"}, {"hash_id", "String"}}
 * @param argc passed from main()'s argc
 * @param argv passed from main()'s argv
 * @return 0 if successfull or error code otherwise
 * @details if argv has --rewrite drops current table
 */
[[nodiscard]] int uploadDriversData(clickhouse::Client& client,
    std::string_view db_name,
    std::string_view table_name,
    const ClickhouseFiller::scheme_t& scheme,
    int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_drivers.empty()) {
        std::cerr << "data file path can't be empty" << std::endl;
        return EINVAL;
    }
    try {
        ClickhouseFiller filler(client, db_name);
        if (FLAGS_rewrite) {
            filler.DropTable();
        }
        filler.CreateTable(table_name, scheme);
        auto [pushed, duplicated] = filler.Add(FLAGS_drivers);
        std::cout << "pushed: " << pushed
                  << "; duplicated: " << duplicated << std::endl;

    }  catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    return 0;
}

int main(int argc, char **argv)
{
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    std::string_view db_name{"test"};
    std::string_view table_name{"drivers"};
    ClickhouseFiller::scheme_t table_scheme{
        {"id", "UInt64"}, {"hash_id", "String"}
    };
    int res = uploadDriversData(client, db_name, table_name, table_scheme, argc, argv);

    if (res)
        exit(res);
    return 0;
}
