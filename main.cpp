#include <iostream>
#include <string_view>

#include "uploadDriversData.hpp"

using namespace std;

int main(int argc, char **argv)
{
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost("192.168.1.21")
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
