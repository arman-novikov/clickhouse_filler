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
                     const scheme_t& scheme = {},
                     const std::string& data_file = "");

    void CreateTable(std::string_view table_name = "",
                      const scheme_t& scheme = {});
    src_data_set_t Add(const std::string& data_file);

    ~ClickhouseFiller() = default;
private:    
    typedef std::vector<src_data_t> read_data_t;

    static std::string GetCreationScheme(const scheme_t& scheme);
    static std::string GetSelectScheme(const scheme_t& scheme);

    ///> todo: use stategy pattern?
    read_data_t ReadFile(const std::string& data_file) const;
    read_data_t ParseJson(std::ifstream& file) const;
    read_data_t ParseCsv(std::ifstream& file) const;
    void Validate(const src_data_t& data) const;
    ///<

    void CreateDb();
    void DropTable();

    /// todo: implement for each type using templates ?
    uint64_t Select(src_data_set_t& container);

    clickhouse::Client* client_;
    std::string db_name_;
    std::string table_name_;
    scheme_t scheme_;
};
