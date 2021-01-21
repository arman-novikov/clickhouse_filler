#include <iostream>
#include <string_view>

#include "ClickhouseFiller.hpp"

using namespace std;

const std::string clickhuse_host{"192.168.1.19"};
std::string_view db_name{"test"};
std::string_view table_name{"drivers"};
ClickhouseFiller::scheme_t table_scheme{
    {"id", "UInt64"}, {"hash_id", "String"}
};

void filler_ctor_test() {
    ClickhouseFiller filler(clickhuse_host,
                            db_name,
                            table_name,
                            table_scheme);
}

void filler_create_db_test() {
    ClickhouseFiller filler(clickhuse_host, db_name);
}

void filler_create_table_test() {
    ClickhouseFiller filler(clickhuse_host, db_name);
    filler.create_table(table_name, table_scheme);
}

void filler_drop_table_test() {
    ClickhouseFiller filler(clickhuse_host, db_name);
    filler.create_table(table_name, table_scheme);
    filler.create_table("table", table_scheme);
}

void filler_read_test() {
    ClickhouseFiller filler(clickhuse_host, db_name);
    filler.create_table(table_name, table_scheme);
    filler.add("data.csv");
    filler.add("extra.csv");
    filler.add("dupl.csv");
}

void filler_reread_test() {
    ClickhouseFiller filler(clickhuse_host, db_name);
    filler.create_table(table_name, table_scheme);
    filler.add("data.csv");
    int x; cin >> x;
    filler.create_table(table_name, table_scheme);
    filler.add("extra.csv");
    filler.add("dupl.csv");
}


void filler_read_json_test() {
    ClickhouseFiller filler(clickhuse_host, db_name);
    filler.create_table(table_name, table_scheme);
    filler.add("data.json");
}


int main()
{
    filler_read_json_test();
    return 0;
}
