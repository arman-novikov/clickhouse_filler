/*
 * File:   ClickhouseFiller.hpp
 * Author: armannovikov
 *
 * Created on 19 января 2021 г., 16:29
 */
#pragma once
#include <string_view>
#include <string>
#include <vector>
#include <utility>
#include <unordered_set>
#include <fstream>
#include <memory>
#include <clickhouse/client.h>

class ClickhouseFiller final {
public:
    typedef std::vector<std::pair<std::string, std::string>> scheme_t;
    typedef std::string src_data_t;
    typedef std::unordered_set<ClickhouseFiller::src_data_t>
        src_data_set_t;
    ClickhouseFiller(clickhouse::Client& client,
                     std::string_view db_name,
                     std::string_view table_name = "",
                     const ClickhouseFiller::scheme_t& scheme = {},
                     const std::string& data_file = "");

    void create_table(std::string_view table_name = "",
                      const ClickhouseFiller::scheme_t& scheme = {});    
    src_data_set_t add(const std::string& data_file);

    ~ClickhouseFiller();
private:    
    typedef std::vector<ClickhouseFiller::src_data_t> read_data_t;

    static std::string get_creation_scheme(
            const ClickhouseFiller::scheme_t& scheme);
    static std::string get_select_scheme(
            const ClickhouseFiller::scheme_t& scheme);

    ///> todo: use stategy pattern?
    ClickhouseFiller::read_data_t
        read_file(const std::string& data_file) const;
    ClickhouseFiller::read_data_t
        parse_json(std::ifstream& file) const;
    ClickhouseFiller::read_data_t
        parse_csv(std::ifstream& file) const;
    void validate(const ClickhouseFiller::src_data_t& data) const;
    ///<

    void create_db();
    void drop_table();

    /// todo: implement for each type using templates ?
    uint64_t select(ClickhouseFiller::src_data_set_t& container);

    clickhouse::Client* client_;
    std::string db_name_;
    std::string table_name_;
    ClickhouseFiller::scheme_t scheme_;
};
