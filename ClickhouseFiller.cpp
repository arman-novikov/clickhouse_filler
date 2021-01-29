/*
 * File:   ClickhouseFiller.cpp
 * Author: armannovikov
 *
 * Created on 19 января 2021 г., 16:29
 */
#include "ClickhouseFiller.hpp"
#include "nlohmann_json/json.hpp"

#include <fmt/format.h>
#include <fmt/compile.h>

namespace ch = clickhouse;
/*!
* @brief creates a table in DB and fills with data from a supplied file
* @param client Clickhouse client
* @param db_name name of data base to be used
* @param table_name table to be created and filled
* @param scheme a vector of string pairs. example:
*   {{"id", "UInt64"}, {"hash_id", "String"}}
* @param data_file path to a file to take data from (json, csv)
* @details
*	- creates DB if not exists
*	- deletes table (if exists) and creates a new one
*	- validate data from a supplied file and pushes data to the table
*/
ClickhouseFiller::ClickhouseFiller(
            ch::Client& client,
            std::string_view db_name,
            std::string_view table_name,
            const ClickhouseFiller::scheme_t& scheme,
            const std::string& data_file):
    client_(&client),
    db_name_{db_name}, table_name_{table_name}, scheme_{scheme}
{
    CreateDb();
    if (table_name.empty() || scheme_.size() == 0) {
        return;
    }
    CreateTable();
    if (data_file.length()) {
        Add(data_file);
    }
}

void ClickhouseFiller::CreateDb() {
    std::string query(fmt::format(
        FMT_COMPILE("CREATE DATABASE IF NOT EXISTS {}"), db_name_)
    );
    client_->Execute(query);
}

/*!
 * @brief creates table if not exists
 * @param table_name
 * @param scheme a vector of string pairs. example:
 *   {{"id", "UInt64"}, {"hash_id", "String"}}
 * @throw clickhouse::ServerException
 * @details if scheme is faulty throws clickhouse::ServerException
 */
void ClickhouseFiller::CreateTable(std::string_view table_name,
                                   const ClickhouseFiller::scheme_t& scheme) {
    if (!table_name.empty()) {
        table_name_ = table_name;
    }
    if (scheme.size()) {
        scheme_ = scheme;
    }
    /// todo: parametrize ENGINE ?
    std::string query(fmt::format(
        FMT_COMPILE("CREATE TABLE IF NOT EXISTS {}.{} {}  ENGINE = Memory"),
        db_name_, table_name_, GetCreationScheme(scheme_))
    );
    client_->Execute(query);
}

/*!
 * @brief inserts data from file into table
 * @param data_file file to read data from
 * @return a number of inserted and a number of duplicated values
 * @warning make sure a table is created
 * @todo make it type generic
 */
std::pair<size_t, size_t> ClickhouseFiller::Add(const std::string& data_file) {
    read_data_t data_to_add = ReadFile(data_file);
    src_data_set_t current_data;
    uint64_t current_max_id = Select(current_data);
    std::vector<uint64_t> ids;
    decltype(data_to_add) hash_ids{}, duplicates{};

    for (const auto& value: data_to_add) {
        if(current_data.find(value) == current_data.end()) {
            hash_ids.push_back(value);
            ids.push_back(++current_max_id);
        } else {
            duplicates.push_back(value);
        }
    }

    ch::Block block;
    auto ids_column = std::make_shared<ch::ColumnUInt64>(ids);
    auto hash_ids_column = std::make_shared<ch::ColumnString>(hash_ids);
    block.AppendColumn(scheme_[0].first  , ids_column);
    block.AppendColumn(scheme_[1].first, hash_ids_column);
    client_->Insert(
        fmt::format(FMT_COMPILE("{}.{}"), db_name_, table_name_),
        block
    );
    return std::make_pair<>(hash_ids.size(), duplicates.size());
}

/*!
 * @brief makes string to create columns
 * @return string like "(id UInt64, name String)"
 */
std::string ClickhouseFiller::GetCreationScheme(
        const ClickhouseFiller::scheme_t& scheme) {
    std::vector<std::string> column_names_types;
    for (const auto& column_type: scheme) {
        column_names_types.push_back(fmt::format(FMT_COMPILE("{} {}"),
            column_type.first, column_type.second));
    }
    std::string res{
        fmt::format(FMT_COMPILE("({})"),
                    fmt::join(column_names_types, FMT_COMPILE(", ")))
    };
    return res;
}

/*!
 * @brief makes string to select all columns
 * @return string like "id, name"
 */
std::string ClickhouseFiller::GetSelectScheme(
        const ClickhouseFiller::scheme_t& scheme){
    std::vector<std::string> column_names;
    for (const auto& column_type: scheme) {
        column_names.push_back(fmt::format(FMT_COMPILE("{}"),
            column_type.first));
    }
    std::string res{
        fmt::format(FMT_COMPILE("{}"),
                    fmt::join(column_names, FMT_COMPILE(", ")))
    };
    return res;
}

/*!
 * @brief selects current data from table and returns it
 * @param [out] container destination
 * @return max id in table
 */
uint64_t
ClickhouseFiller::Select(ClickhouseFiller::src_data_set_t& container) {
    uint64_t current_max_id{0};
    std::string select_query(
        fmt::format(FMT_COMPILE("SELECT {} FROM {}.{} ORDER BY id"), /// todo: parametrize ordering
            GetSelectScheme(scheme_), db_name_, table_name_)
    );
    auto on_select = [&] (const ch::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            container.insert(
               std::string{block[1]->As<ch::ColumnString>()->At(i)} ///> HARDCODE assumings it contains ID
            );
            auto id = block[0]->As<ch::ColumnUInt64>()->At(i);  ///> HARDCODE assuming it contains hash_id
            current_max_id = std::max(id, current_max_id);
        }
    };
    client_->Select(select_query, on_select);
    return current_max_id;
}

/*!
 * @brief reads file and chooses a parser
 * @param data_file [path] + file name
 * @return parsed data
 * @throw std::runtime_error if can't open the file
 */
ClickhouseFiller::read_data_t
ClickhouseFiller::ReadFile(const std::string& data_file) const {
    read_data_t res;
    std::ifstream file(data_file);
    if (!file.is_open()) {
        throw std::runtime_error("can't open file " + data_file);
    }
    if(data_file.substr(data_file.find_last_of(".") + 1) == "json") {
        res = ParseJson(file);
    } else {
        res = ParseCsv(file);
    }
    for (const auto& val: res) {
        Validate(val);
    }
    return res;
}

/*!
 * @brief reads and parses json file
 * @param data_file path to a json file
 * @return vectorized data from file
 * @details file as {"data": {"drivers": [..., ...]} }
 */
ClickhouseFiller::read_data_t
ClickhouseFiller::ParseJson(std::ifstream& file) const {
    nlohmann::json j;
    file >> j;
    return j["data"]["drivers"].get<ClickhouseFiller::read_data_t>();
}

/*!
 * @brief reads file line by line
 * @param data_file path to the csv file
 * @return vectorized data from file
 */
ClickhouseFiller::read_data_t
ClickhouseFiller::ParseCsv(std::ifstream& file) const
{
    std::vector<ClickhouseFiller::src_data_t> res;
    for (std::string line; std::getline(file, line);)
        res.push_back(line);
    return res;
}

void ClickhouseFiller::Validate(const ClickhouseFiller::src_data_t& data) const
{
    using namespace std::string_literals;
    if(data.empty()) {
        throw std::runtime_error("validation failed for: "s + data);
    }
}

/*!
 * @brief drops table named with table_name_
 */
void ClickhouseFiller::DropTable() {
    if (table_name_.empty()) {
        return;
    }
    std::string cmd{
        fmt::format(FMT_COMPILE("DROP TABLE IF EXISTS {}.{}"),
                    db_name_, table_name_)
    };
}
