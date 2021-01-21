cmake_minimum_required(VERSION 3.5)

set(PROJECT_NAME clickhousefiller)
project(clickhousefiller LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(clickhousefiller
    main.cpp
    ClickhouseFiller.cpp
    ClickhouseFiller.hpp
    nlohmann_json/json.hpp
)

target_link_libraries(
    ${PROJECT_NAME}
    clickhouse-cpp-lib-static cityhash-lib lz4-lib
    fmt
)
