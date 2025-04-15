#include <db/ColumnStats.hpp>
#include <algorithm>
#include <cmath>
#include <stdexcept>

using namespace db;

ColumnStats::ColumnStats(unsigned b, int mn, int mx)
    : numBuckets(b), minVal(mn), maxVal(mx), totalCount(0) {
    if (b == 0 || mx < mn) {
        throw std::invalid_argument("Invalid bucket or range");
    }
    histogram.assign(numBuckets, 0);
    // no floating bucketWidth; we'll compute each bucket start/end as integers
}

void ColumnStats::addValue(int v) {
    if (v < minVal || v > maxVal) return;
    int bucketSize = (maxVal - minVal + 1) / numBuckets;
    if (bucketSize < 1) bucketSize = 1;
    int index = (v - minVal) / bucketSize;
    if (index >= (int)numBuckets) index = numBuckets - 1;
    histogram[index]++;
    totalCount++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
    if (totalCount == 0) return 0;
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
    int bucketSize = (maxVal - minVal + 1) / numBuckets;
    if (bucketSize < 1) bucketSize = 1;
    int idx = (v - minVal) / bucketSize;
    if (idx >= (int)numBuckets) idx = numBuckets - 1;
    int bStart = minVal + idx * bucketSize;
    int bEnd = bStart + bucketSize - 1;
    if (bEnd > maxVal) bEnd = maxVal;
    double h = histogram[idx];
    switch (op) {
        case PredicateOp::EQ: {
            int width = (bEnd - bStart + 1);
            double d = width > 0 ? h / width : 0.0;
            return (size_t)(d);
        }
        case PredicateOp::NE: {
            size_t eqc = estimateCardinality(PredicateOp::EQ, v);
            return totalCount > eqc ? totalCount - eqc : 0;
        }
        case PredicateOp::GT: {
            int width = (bEnd - bStart + 1);
            double frac = (bEnd - v) / double(width);
            if (frac < 0) frac = 0;
            double part = h * frac;
            size_t sumNext = 0;
            for (int i = idx + 1; i < (int)numBuckets; i++) {
                sumNext += histogram[i];
            }
            return (size_t)(part + sumNext);
        }
        case PredicateOp::GE: {
            // same as GT, but we shift fraction so it includes v itself
            int width = (bEnd - bStart + 1);
            double frac = (bEnd - v + 1) / double(width);
            if (frac < 0) frac = 0;
            double part = h * frac;
            size_t sumNext = 0;
            for (int i = idx + 1; i < (int)numBuckets; i++) {
                sumNext += histogram[i];
            }
            return (size_t)(part + sumNext);
        }
        case PredicateOp::LT: {
            int width = (bEnd - bStart + 1);
            double frac = (v - bStart) / double(width);
            if (frac < 0) frac = 0;
            double part = h * frac;
            size_t sumBefore = 0;
            for (int i = 0; i < idx; i++) {
                sumBefore += histogram[i];
            }
            return (size_t)(sumBefore + part);
        }
        case PredicateOp::LE: {
            // similar to LT, but includes v
            int width = (bEnd - bStart + 1);
            double frac = (v - bStart + 1) / double(width);
            if (frac < 0) frac = 0;
            if (frac > 1.0) frac = 1.0;
            double part = h * frac;
            size_t sumBefore = 0;
            for (int i = 0; i < idx; i++) {
                sumBefore += histogram[i];
            }
            return (size_t)(sumBefore + part);
        }
    }
    return 0;
}
