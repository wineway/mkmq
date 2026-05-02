cmake_minimum_required(VERSION 3.25)
if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()

project(mercury_kafka_mq
    VERSION 0.2.0
    DESCRIPTION "Seastar/Mercury HFT Kafka-compatible message queue"
    LANGUAGES CXX)

option(MKMQ_ENABLE_MERCURY "Build Mercury broker-to-broker transport" ON)
option(MKMQ_HOTPATH_DEBUG_LOGS "Enable debug logs on Kafka/Mercury hot paths" OFF)
option(MKMQ_ENABLE_INTEGRATION_TESTS "Enable broker process integration tests" OFF)
set(SEASTAR_ROOT "" CACHE PATH "Seastar build or install prefix, e.g. /seastar/build/debug")

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_path(MKMQ_SEASTAR_MODULE_DIR
    NAMES SeastarDependencies.cmake
    HINTS
        "${SEASTAR_ROOT}"
        "${Seastar_DIR}"
    PATH_SUFFIXES
        cmake
        ../cmake
        ../../cmake
        ../../../cmake)
if(MKMQ_SEASTAR_MODULE_DIR)
    list(APPEND CMAKE_MODULE_PATH "${MKMQ_SEASTAR_MODULE_DIR}")
endif()

if(SEASTAR_ROOT)
    find_package(Seastar REQUIRED CONFIG
        PATHS "${SEASTAR_ROOT}" "${SEASTAR_ROOT}/lib/cmake/Seastar"
        NO_DEFAULT_PATH)
else()
    find_package(Seastar REQUIRED CONFIG)
endif()

find_package(yaml-cpp REQUIRED CONFIG)
find_package(CRC32C REQUIRED)

if(MKMQ_ENABLE_MERCURY)
    find_package(Mercury REQUIRED)
endif()

add_library(mkmq_protocol
    src/crc32c.cpp
    src/generated/kafka_codec.cpp
    src/generated/kafka_schema.cpp
    src/protocol/api.cpp
    src/protocol/header.cpp
    src/protocol/wire.cpp)

target_include_directories(mkmq_protocol
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_compile_features(mkmq_protocol PUBLIC cxx_std_23)
target_link_libraries(mkmq_protocol PUBLIC CRC32C::crc32c)

add_library(mkmq_core
    src/broker_shard.cpp
    src/config.cpp
    src/kafka_protocol.cpp
    src/mercury_shard.cpp)

target_include_directories(mkmq_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_compile_features(mkmq_core PUBLIC cxx_std_23)
target_link_libraries(mkmq_core
    PUBLIC
        mkmq_protocol
        Seastar::seastar
        yaml-cpp::yaml-cpp
        CRC32C::crc32c)

if(MKMQ_ENABLE_MERCURY)
    target_link_libraries(mkmq_core PUBLIC Mercury::Mercury)
    target_compile_definitions(mkmq_core PUBLIC MKMQ_HAVE_MERCURY=1)
else()
    target_compile_definitions(mkmq_core PUBLIC MKMQ_HAVE_MERCURY=0)
endif()

target_compile_definitions(mkmq_core
    PUBLIC
        MKMQ_HOTPATH_DEBUG_LOGS=$<BOOL:${MKMQ_HOTPATH_DEBUG_LOGS}>)

add_executable(mkmq_broker src/main.cpp)
target_link_libraries(mkmq_broker PRIVATE mkmq_core)

add_executable(mkmq_kafka_bench src/tools/kafka_bench_client.cpp)
target_link_libraries(mkmq_kafka_bench PRIVATE mkmq_protocol)

include(CTest)
if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter)
    if(Python3_Interpreter_FOUND)
        add_test(NAME mkmq_schema_generator_selftest
            COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/tests/schema_generator_selftest.py" "${CMAKE_CURRENT_SOURCE_DIR}")
    endif()

    add_executable(mkmq_selftest
        tests/selftest.cpp)
    target_link_libraries(mkmq_selftest PRIVATE mkmq_core)
    add_test(NAME mkmq_selftest
        COMMAND mkmq_selftest --smp 1 --memory 512M --overprovisioned)
    if(MKMQ_ENABLE_INTEGRATION_TESTS)
        add_test(NAME mkmq_reload_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_reload.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
        add_test(NAME mkmq_rf3_degraded_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_rf3_degraded.sh" "$<TARGET_FILE_DIR:mkmq_broker>" "${CMAKE_CURRENT_SOURCE_DIR}/configs/three-broker-rf3.yaml")
        add_test(NAME mkmq_fetch_wait_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_fetch_wait.sh" "$<TARGET_FILE_DIR:mkmq_broker>" "${CMAKE_CURRENT_SOURCE_DIR}/configs/single-broker.yaml")
        add_test(NAME mkmq_fetch_visibility_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_fetch_visibility.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
        add_test(NAME mkmq_unsupported_produce_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_unsupported_produce.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
        add_test(NAME mkmq_acks_zero_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_acks_zero.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
        add_test(NAME mkmq_no_auto_create_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_no_auto_create.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
        add_test(NAME mkmq_segment_rollover_integration
            COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_segment_rollover.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
        if(MKMQ_ENABLE_MERCURY)
            add_test(NAME mkmq_rf3_mercury_integration
                COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_rf3_mercury.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
            add_test(NAME mkmq_rf3_catchup_integration
                COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_rf3_catchup.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
            add_test(NAME mkmq_produce_timeout_integration
                COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_produce_timeout.sh" "$<TARGET_FILE_DIR:mkmq_broker>")
        endif()
    endif()
endif()
