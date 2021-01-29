#pragma once
#include <iostream>
#include "ClickhouseFiller.hpp"

namespace {
const std::string g_clickhuse_host{"192.168.1.21"};
std::string_view g_db_name{"test"};
std::string_view g_table_name{"drivers"};
ClickhouseFiller::scheme_t g_table_scheme{
    {"id", "UInt64"}, {"hash_id", "String"}
};
}

void filler_ctor_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client,
                            g_db_name,
                            g_table_name,
                            g_table_scheme);
}

void filler_create_db_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client, g_db_name);
}

void filler_create_table_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client, g_db_name);
    filler.CreateTable(g_table_name, g_table_scheme);
}

void filler_drop_table_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client, g_db_name);
    filler.CreateTable(g_table_name, g_table_scheme);
    filler.CreateTable("table", g_table_scheme);
}

void filler_read_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client, g_db_name);
    filler.CreateTable(g_table_name, g_table_scheme);
    filler.Add("data.csv");
    filler.Add("extra.csv");
    filler.Add("dupl.csv");
}

void filler_reread_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client, g_db_name);
    filler.CreateTable(g_table_name, g_table_scheme);
    filler.Add("data.csv");
    int x; std::cin >> x;
    filler.CreateTable(g_table_name, g_table_scheme);
    filler.Add("extra.csv");
    filler.Add("dupl.csv");
}


void filler_read_json_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client, g_db_name);
    filler.CreateTable(g_table_name, g_table_scheme);
    filler.Add("data.json");
}

void filler_read_misc_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client, g_db_name);
    filler.CreateTable(g_table_name, g_table_scheme);
    filler.Add("data.csv");
    filler.Add("data.json");
    filler.Add("extra.csv");
    filler.Add("dupl.csv");
}

void filler_ctor_read_misc_test() {
    auto client = clickhouse::Client(
        clickhouse::ClientOptions().SetHost(g_clickhuse_host)
    );
    ClickhouseFiller filler(client,
                            g_db_name,
                            g_table_name,
                            g_table_scheme,
                            "data.csv");
    filler.Add("data.csv");
    filler.Add("data.json");
    filler.Add("extra.csv");
    filler.Add("dupl.csv");
}
