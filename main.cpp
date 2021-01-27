#include <iostream>
#include <string_view>

#include "ClickhouseFiller.hpp"

using namespace std;

const std::string clickhuse_host{"192.168.1.21"};
std::string_view db_name{"test"};
std::string_view table_name{"drivers"};
ClickhouseFiller::scheme_t table_scheme{
    {"id", "UInt64"}, {"hash_id", "String"}
};

void filler_ctor_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client,
                            db_name,
                            table_name,
                            table_scheme);
}

void filler_create_db_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client, db_name);
}

void filler_create_table_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client, db_name);
    filler.create_table(table_name, table_scheme);
}

void filler_drop_table_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client, db_name);
    filler.create_table(table_name, table_scheme);
    filler.create_table("table", table_scheme);
}

void filler_read_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client, db_name);
    filler.create_table(table_name, table_scheme);
    filler.add("data.csv");
    filler.add("extra.csv");
    filler.add("dupl.csv");
}

void filler_reread_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client, db_name);
    filler.create_table(table_name, table_scheme);
    filler.add("data.csv");
    int x; cin >> x;
    filler.create_table(table_name, table_scheme);
    filler.add("extra.csv");
    filler.add("dupl.csv");
}


void filler_read_json_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client, db_name);
    filler.create_table(table_name, table_scheme);
    filler.add("data.json");
}

void filler_read_misc_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client, db_name);
    filler.create_table(table_name, table_scheme);
    filler.add("data.csv");
    filler.add("data.json");
    filler.add("extra.csv");
    filler.add("dupl.csv");
}

void filler_ctor_read_misc_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(clickhuse_host)
    );
    ClickhouseFiller filler(client,
                            db_name,
                            table_name,
                            table_scheme,
                            "data.csv");
    filler.add("data.csv");
    filler.add("data.json");
    filler.add("extra.csv");
    filler.add("dupl.csv");
}

int main()
{
    filler_ctor_read_misc_test();
    return 0;
}
