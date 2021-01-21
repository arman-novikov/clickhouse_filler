/*
 * File:   ClickhouseFiller.cpp
 * Author: armannovikov
 *
 * Created on 19 января 2021 г., 16:29
 */
#include "ClickhouseFiller.hpp"
#include <iostream>
#include "nlohmann_json/json.hpp"

#include <fmt/format.h>
#include <fmt/compile.h>

namespace ch = clickhouse;
/*!
* @brief creates a table in DB and fills with data from a supplied file
* @param db_name name of data base to be used
* @param table_name table to be created and filled
* @param data_file path to a file to take data from (json, csv)
* @details
*	- creates DB if not exists
*	- deletes table (if exists) and creates a new one
*	- validate data from a supplied file and pushes data to the table
*/
ClickhouseFiller::ClickhouseFiller(
            const std::string& clickhouse_url,
            std::string_view db_name,
            std::string_view table_name,
            const ClickhouseFiller::scheme_t& scheme,
            const std::string& data_file) try:
    client_(ch::Client(ch::ClientOptions().SetHost(clickhouse_url))),
    db_name_{db_name}, table_name_{table_name}, scheme_{scheme}
{
    this->create_db();
    this->create_table();
    if (data_file.length()) {
        this->add(data_file);
    }
} catch (std::system_error &err) {
    //throw ch::clickhouse_error(clickhouse_url + " failed to connect", err.what());
}

void ClickhouseFiller::create_db() {
    auto query(fmt::format(
                   FMT_COMPILE("CREATE DATABASE IF NOT EXISTS {}"),
                   this->db_name_)
               );
    this->client_.Execute(query);
}

/*!
 * @brief creates table if not exists
 * @param table_name
 * @param scheme something like "(id UInt64, name String) ENGINE = Memory"
 * @throw clickhouse::ServerException
 * @details if scheme is faulty throws clickhouse::ServerException
 */
void ClickhouseFiller::create_table(std::string_view table_name,
                                    const ClickhouseFiller::scheme_t& scheme) {
    this->drop_table(); // drop the current one
    if (!table_name.empty()) {
        this->table_name_ = table_name;
    }
    if (scheme.size()) {
        this->scheme_ = scheme;
    }

    auto query(fmt::format(
                 FMT_COMPILE(
                    "CREATE TABLE IF NOT EXISTS {}.{} {}  ENGINE = Memory"  /// todo: parametrize ENGINE
                 ),
                 this->db_name_,
                 this->table_name_,
                 this->get_creation_scheme())
               );
    try {
        this->client_.Execute(query);
    } catch (const clickhouse::ServerException& err) {
       using namespace std::string_literals;
       //throw ch::clickhouse_error("failed to create table: "s + err.what());
    }
}

/*!
 * @brief inserts data from file into table
 * @param data_file file to read data from
 * @return set of not passed values
 * @warning make sure a table is created
 * @todo make it type generic and split into methods
 */
ClickhouseFiller::src_data_set_t
ClickhouseFiller::add(const std::string& data_file) {
    auto data_to_add = this->read_file(data_file);
    ClickhouseFiller::src_data_set_t current_data, ignored;
    uint64_t current_max_id = this->select(current_data);

    std::vector<uint64_t> ids;
    decltype(data_to_add) hash_ids;
    for (const auto& value: data_to_add) {
        if(current_data.find(value) == current_data.end()) {
            hash_ids.push_back(value);
            ids.push_back(++current_max_id);
        } else {
            ignored.insert(value);
        }
    }

    {
        ch::Block block;
        auto ids_column = std::make_shared<ch::ColumnUInt64>(ids);
        auto hash_ids_column = std::make_shared<ch::ColumnString>(hash_ids);
        block.AppendColumn(this->scheme_[0].first  , ids_column);
        block.AppendColumn(this->scheme_[1].first, hash_ids_column);
        this->client_.Insert(
            fmt::format(FMT_COMPILE("{}.{}"),
                this->db_name_,
                this->table_name_),
            block
        );
    }

    return ignored;
}

std::string ClickhouseFiller::get_creation_scheme() const {
    std::vector<std::string> column_names_types;
    for (const auto& column_type: this->scheme_) {
        column_names_types.push_back(fmt::format(FMT_COMPILE("{} {}"),
            column_type.first, column_type.second));
    }

    auto res{
        fmt::format(FMT_COMPILE("({})"),
                    fmt::join(column_names_types, FMT_COMPILE(", ")))
    };

    return res;
}

std::string ClickhouseFiller::get_select_scheme() const {
    std::vector<std::string> column_names;
    for (const auto& column_type: this->scheme_) {
        column_names.push_back(fmt::format(FMT_COMPILE("{}"),
            column_type.first));
    }

    auto res{
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
ClickhouseFiller::select(ClickhouseFiller::src_data_set_t& container) {
    uint64_t current_max_id{0};

    auto select_query(
        fmt::format(FMT_COMPILE("SELECT {} FROM {}.{} ORDER BY id"), /// todo: parametrize ordering
            this->get_select_scheme(),
            this->db_name_,
            this->table_name_)
    );

    auto on_select = [&] (const ch::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            container.insert(
               std::string{block[1]->As<ch::ColumnString>()->At(i)} ///> HARDCODE
            );

            auto id = block[0]->As<ch::ColumnUInt64>()->At(i);  ///> HARDCODE
            current_max_id = std::max(id, current_max_id);
        }
    };

    this->client_.Select(select_query, on_select);
    return current_max_id;
}


ClickhouseFiller::read_data_t
ClickhouseFiller::read_file(const std::string& data_file) const {
    ClickhouseFiller::read_data_t res;

    std::ifstream file(data_file);
    if (!file.is_open()) {
        throw std::runtime_error("can't open file " + data_file);
    }

    if(data_file.substr(data_file.find_last_of(".") + 1) == "json") {
        res = this->parse_json(file);
    } else {
        res = this->parse_csv(file);
    }

    for (const auto& val: res) {
        this->validate(val);
    }

    return res;
}


/*!
 * @warning stub!
 * @brief data_file path to the json file
 * @return vector of read elements
 * @return vectorized data from file
 */
ClickhouseFiller::read_data_t
ClickhouseFiller::parse_json(std::ifstream& file) const {
    ClickhouseFiller::read_data_t res;

    nlohmann::json j;
    file >> j;

    std::cout << j << std::endl;

    res = j["data"]["drivers"].get<ClickhouseFiller::read_data_t>();

    for (auto& i: res) {
        std::cout << i << std::endl;
    }
    return res;
}

/*!
 * @brief reads file line by line
 * @param data_file path to the csv file
 * @return vector of read elements
 * @throw std::runtime_error if can't open the file
 */
ClickhouseFiller::read_data_t
ClickhouseFiller::parse_csv(std::ifstream& file) const
{    
    std::vector<ClickhouseFiller::src_data_t> res;
    for (std::string line; std::getline(file, line);)
        res.push_back(line);

    file.close();
    return res;
}

void ClickhouseFiller::validate(const ClickhouseFiller::src_data_t& data) const
{
    using namespace std::string_literals;
    if(data.empty()) {
        throw std::runtime_error("validation failed for: "s + data);
    }
}

void ClickhouseFiller::drop_table() {
    if (this->table_name_.empty()) {
        return;
    }
    auto cmd{fmt::format(FMT_COMPILE("DROP TABLE {}.{}"),
                         this->db_name_, this->table_name_)};
    try {
        this->client_.Execute(cmd);
    }  catch (const ch::ServerException&) {
        // if not exists and nothing to delete that is ok
    }
}

ClickhouseFiller::~ClickhouseFiller() {
    //this->drop_table();
}
