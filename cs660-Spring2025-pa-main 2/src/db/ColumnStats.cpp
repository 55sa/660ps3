#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <db/ColumnStats.hpp>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
    : numBuckets(buckets), minVal(min), maxVal(max), totalCount(0) {
    if (buckets == 0 || max < min) {
        throw std::invalid_argument("Invalid bucket count or range");
    }
    bucketWidth = double(max - min + 1) / numBuckets;
    histogram.assign(numBuckets, 0);
}

void ColumnStats::addValue(int v) {
    if (v < minVal || v > maxVal) {
        return;
    }
    unsigned idx = unsigned((v - minVal) / bucketWidth);
    if (idx >= numBuckets) {
        idx = numBuckets - 1;
    }
    histogram[idx]++;
    totalCount++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
    if (totalCount == 0) {
        return 0;
    }
    if (v < minVal) {
        if (op == PredicateOp::LT || op == PredicateOp::LE) return 0;
        if (op == PredicateOp::GT || op == PredicateOp::GE) return totalCount;
        if (op == PredicateOp::EQ) return 0;
        if (op == PredicateOp::NE) return totalCount;
    }
    if (v > maxVal) {
        if (op == PredicateOp::GT || op == PredicateOp::GE) return 0;
        if (op == PredicateOp::LT || op == PredicateOp::LE) return totalCount;
        if (op == PredicateOp::EQ) return 0;
        if (op == PredicateOp::NE) return totalCount;
    }
    unsigned b = unsigned((v - minVal) / bucketWidth);
    if (b >= numBuckets) {
        b = numBuckets - 1;
    }
    double lb = minVal + b * bucketWidth;
    double ub = lb + bucketWidth;
    double c = histogram[b];
    size_t est = 0;
    switch (op) {
        case PredicateOp::EQ: {
            double d = c / bucketWidth;
            est = size_t(d);
            break;
        }
        case PredicateOp::NE: {
            double d = c / bucketWidth;
            size_t eqEst = size_t(d);
            est = totalCount > eqEst ? totalCount - eqEst : 0;
            break;
        }
        case PredicateOp::GT: {
            double frac = (ub - v) / bucketWidth;
            double cnt = c * frac;
            size_t after = 0;
            for (unsigned i = b + 1; i < numBuckets; i++) {
                after += histogram[i];
            }
            est = size_t(cnt + after);
            break;
        }
        case PredicateOp::GE: {
            double frac = (ub - v) / bucketWidth;
            double cnt = c * frac;
            size_t after = 0;
            for (unsigned i = b + 1; i < numBuckets; i++) {
                after += histogram[i];
            }
            est = size_t(cnt + after);
            break;
        }
        case PredicateOp::LT: {
            double frac = (v - lb) / bucketWidth;
            double cnt = c * frac;
            size_t before = 0;
            for (unsigned i = 0; i < b; i++) {
                before += histogram[i];
            }
            est = size_t(before + cnt);
            break;
        }
        case PredicateOp::LE: {
            double frac = (v - lb + 1) / bucketWidth;
            if (frac > 1.0) frac = 1.0;
            double cnt = c * frac;
            size_t before = 0;
            for (unsigned i = 0; i < b; i++) {
                before += histogram[i];
            }
            est = size_t(before + cnt);
            break;
        }
        default:
            throw std::logic_error("Unsupported predicate");
    }
    return est;
}
