#pragma once
#include "ClickhouseFiller.hpp"

[[nodiscard]] int uploadDriversData(clickhouse::Client& client,
    std::string_view db_name,
    std::string_view table_name,
    const ClickhouseFiller::scheme_t& scheme,
    int argc, char** argv
);
