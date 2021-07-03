//
// Created by Chuying He on 02/07/2021.
//

#include "duckdb/skinnerdb/timer.hpp"

Timer::Timer() {
    m_StartTimePoint = std::chrono::high_resolution_clock::now();
}
Timer::~Timer() {
    // stop();
}

double Timer::check() {
    auto currentTimePoint = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(m_StartTimePoint).time_since_epoch().count();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(currentTimePoint).time_since_epoch().count();

    auto duration = end - start;
    double ms = duration*0.001;

    return ms;
}

/*void Timer::stop() {
    auto endTimePoint = std::chrono::high_resolution_clock::now();

    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(m_StartTimePoint).time_since_epoch().count();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(endTimePoint).time_since_epoch().count();

    auto duration = end - start;
    double ms = duration*0.001;

    std::cout << "\n" <<duration <<" us (" <<ms<<" ms)" << std::endl;
}*/
