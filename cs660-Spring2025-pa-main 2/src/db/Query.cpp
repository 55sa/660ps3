#include <db/Query.hpp>
#include <db/DbFile.hpp>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <vector>
#include <string>
#include <unordered_map>

using namespace db;

static bool evaluatePredicate(const Tuple &t, const FilterPredicate &p, const TupleDesc &td) {
    size_t i = td.index_of(p.field_name);
    int tv = std::get<int>(t.get_field(i));
    int pv = std::get<int>(p.value);
    switch(p.op) {
        case PredicateOp::EQ: return tv == pv;
        case PredicateOp::NE: return tv != pv;
        case PredicateOp::LT: return tv < pv;
        case PredicateOp::LE: return tv <= pv;
        case PredicateOp::GT: return tv > pv;
        case PredicateOp::GE: return tv >= pv;
        default: return false;
    }
}

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
    for (Iterator it = in.begin(); it != in.end(); ++it) {
        Tuple t = *it;
        std::vector<field_t> v;
        for (auto &fn : field_names) {
            size_t i = in.getTupleDesc().index_of(fn);
            v.push_back(t.get_field(i));
        }
        out.insertTuple(Tuple(v));
    }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
    const TupleDesc &td = in.getTupleDesc();
    for (Iterator it = in.begin(); it != in.end(); ++it) {
        Tuple t = *it;
        bool ok = true;
        for (auto &fp : pred) {
            if (!evaluatePredicate(t, fp, td)) {
                ok = false;
                break;
            }
        }
        if (ok) out.insertTuple(t);
    }
}

struct AggResult {
    int count;
    int sum;
    int minv;
    int maxv;
    AggResult() : count(0), sum(0), minv(0), maxv(0) {}
};

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
    const TupleDesc &td = in.getTupleDesc();
    size_t ai = td.index_of(agg.field);
    bool g = agg.group.has_value();
    size_t gi = 0;
    if (g) gi = td.index_of(agg.group.value());
    if (!g) {
        AggResult r;
        bool f = true;
        for (Iterator it = in.begin(); it != in.end(); ++it) {
            Tuple t = *it;
            int v = std::get<int>(t.get_field(ai));
            r.count++;
            r.sum += v;
            if (f) {
                r.minv = v;
                r.maxv = v;
                f = false;
            } else {
                if (v < r.minv) r.minv = v;
                if (v > r.maxv) r.maxv = v;
            }
        }
        std::vector<field_t> vf;
        switch (agg.op) {
            case AggregateOp::COUNT: vf.push_back(r.count); break;
            case AggregateOp::SUM: vf.push_back(r.sum); break;
            case AggregateOp::AVG: vf.push_back((r.count > 0) ? double(r.sum)/r.count : 0.0); break;
            case AggregateOp::MIN: vf.push_back(r.minv); break;
            case AggregateOp::MAX: vf.push_back(r.maxv); break;
            default: throw std::logic_error("unsupported");
        }
        out.insertTuple(Tuple(vf));
    } else {
        std::unordered_map<int, AggResult> m;
        for (Iterator it = in.begin(); it != in.end(); ++it) {
            Tuple t = *it;
            int gv = std::get<int>(t.get_field(gi));
            int v = std::get<int>(t.get_field(ai));
            if (!m.count(gv)) {
                AggResult ar;
                ar.count = 1;
                ar.sum = v;
                ar.minv = v;
                ar.maxv = v;
                m[gv] = ar;
            } else {
                auto &ar = m[gv];
                ar.count++;
                ar.sum += v;
                if (v < ar.minv) ar.minv = v;
                if (v > ar.maxv) ar.maxv = v;
            }
        }
        for (auto &p : m) {
            std::vector<field_t> vf;
            vf.push_back(p.first);
            switch (agg.op) {
                case AggregateOp::COUNT: vf.push_back(p.second.count); break;
                case AggregateOp::SUM: vf.push_back(p.second.sum); break;
                case AggregateOp::AVG: vf.push_back((p.second.count > 0) ? double(p.second.sum)/p.second.count : 0.0); break;
                case AggregateOp::MIN: vf.push_back(p.second.minv); break;
                case AggregateOp::MAX: vf.push_back(p.second.maxv); break;
                default: throw std::logic_error("unsupported");
            }
            out.insertTuple(Tuple(vf));
        }
    }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
    const TupleDesc &ltd = left.getTupleDesc();
    const TupleDesc &rtd = right.getTupleDesc();
    size_t li = ltd.index_of(pred.left);
    size_t ri = rtd.index_of(pred.right);
    for (Iterator lit = left.begin(); lit != left.end(); ++lit) {
        Tuple lt = *lit;
        int lv = std::get<int>(lt.get_field(li));
        for (Iterator rit = right.begin(); rit != right.end(); ++rit) {
            Tuple rt = *rit;
            int rv = std::get<int>(rt.get_field(ri));
            bool match = false;
            switch (pred.op) {
                case PredicateOp::EQ: match = (lv == rv); break;
                case PredicateOp::NE: match = (lv != rv); break;
                case PredicateOp::LT: match = (lv < rv); break;
                case PredicateOp::LE: match = (lv <= rv); break;
                case PredicateOp::GT: match = (lv > rv); break;
                case PredicateOp::GE: match = (lv >= rv); break;
                default: break;
            }
            if (match) {
                std::vector<field_t> mf;
                for (size_t i = 0; i < lt.size(); i++) mf.push_back(lt.get_field(i));
                for (size_t i = 0; i < rt.size(); i++) {
                    if (pred.op == PredicateOp::EQ && i == ri) continue;
                    mf.push_back(rt.get_field(i));
                }
                out.insertTuple(Tuple(mf));
            }
        }
    }
}
