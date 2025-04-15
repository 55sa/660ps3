#ifndef COLUMNSTATS_HPP
#define COLUMNSTATS_HPP

#include <vector>
#include <stdexcept>
#include "db/Query.hpp"  // 包含 PredicateOp 的定义

namespace db {

    /**
     * @brief 用于对单个整型字段建立固定宽度直方图的类。
     *
     * 该类将 [min, max] 范围划分为若干个桶，每个桶统计在该范围内的数值个数。
     */
    class ColumnStats {
    public:
        /**
         * @brief 构造函数
         * @param buckets 直方图的桶数
         * @param min     处理的最小整数值
         * @param max     处理的最大整数值
         */
        ColumnStats(unsigned buckets, int min, int max);

        /**
         * @brief 向直方图中添加一个值
         * @param v 要添加的整数值
         */
        void addValue(int v);

        /**
         * @brief 根据谓词和给定值估计满足条件的元组数（选择性）。
         * @param op  谓词操作（如 EQ、GT 等）
         * @param v   比较的值
         * @return    估计满足条件的元组数
         */
        size_t estimateCardinality(PredicateOp op, int v) const;


        unsigned numBuckets;           // 桶的数量
        int minVal;                    // 处理值的最小值
        int maxVal;                    // 处理值的最大值
        double bucketWidth;            // 每个桶的宽度
        std::vector<size_t> histogram; // 存储各桶计数
        size_t totalCount;             // 添加的总值数量
    };

} // namespace db

#endif // COLUMNSTATS_HPP
