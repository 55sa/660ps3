#include <db/Query.hpp>
#include <db/DbFile.hpp>
#include <db/Tuple.hpp>

#include <unordered_map>
#include <string>
#include <cmath>

using namespace db;

static bool evalFilter(const Tuple &t, const FilterPredicate &p, const TupleDesc &td) {
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

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &fields) {
    for (Iterator it = in.begin(); it != in.end(); ++it) {
        Tuple t = *it;
        std::vector<field_t> vals;
        for (auto &f : fields) {
            size_t idx = in.getTupleDesc().index_of(f);
            vals.push_back(t.get_field(idx));
        }
        out.insertTuple(Tuple(vals));
    }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &preds) {
    const TupleDesc &td = in.getTupleDesc();
    for (Iterator it = in.begin(); it != in.end(); ++it) {
        Tuple t = *it;
        bool pass = true;
        for (auto &p : preds) {
            if (!evalFilter(t, p, td)) {
                pass = false;
                break;
            }
        }
        if (pass) out.insertTuple(t);
    }
}

struct AggRes {
    int sum;
    int minv;
    int maxv;
    int count;
    AggRes() : sum(0), minv(0), maxv(0), count(0) {}
};

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
    const TupleDesc &td = in.getTupleDesc();
    size_t aggIdx = td.index_of(agg.field);
    bool hasGroup = agg.group.has_value();
    std::string gField;
    if (hasGroup) gField = agg.group.value();

    if (!hasGroup) {
        AggRes r;
        bool first = true;
        for (Iterator it = in.begin(); it != in.end(); ++it) {
            Tuple t = *it;
            int v = std::get<int>(t.get_field(aggIdx));
            r.sum += v;
            r.count++;
            if (first) {
                r.minv = v;
                r.maxv = v;
                first = false;
            } else {
                if (v < r.minv) r.minv = v;
                if (v > r.maxv) r.maxv = v;
            }
        }
        std::vector<field_t> val;
        switch (agg.op) {
            case AggregateOp::COUNT: val.push_back(r.count); break;
            case AggregateOp::SUM:   val.push_back(r.sum);   break;
            case AggregateOp::MIN:   val.push_back(r.minv);  break;
            case AggregateOp::MAX:   val.push_back(r.maxv);  break;
            case AggregateOp::AVG: {
                double d = (r.count == 0) ? 0.0 : double(r.sum) / r.count;
                val.push_back(d);
                break;
            }
        }
        out.insertTuple(Tuple(val));
    } else {
        size_t groupIdx = td.index_of(gField);
        type_t gtype = td.field_type(groupIdx);
        if (gtype == type_t::INT) {
            std::unordered_map<int, AggRes> m;
            for (Iterator it = in.begin(); it != in.end(); ++it) {
                Tuple t = *it;
                int gv = std::get<int>(t.get_field(groupIdx));
                int av = std::get<int>(t.get_field(aggIdx));
                auto &r = m[gv];
                if (r.count == 0) {
                    r.minv = av;
                    r.maxv = av;
                } else {
                    if (av < r.minv) r.minv = av;
                    if (av > r.maxv) r.maxv = av;
                }
                r.sum += av;
                r.count++;
            }
            for (auto &kv : m) {
                std::vector<field_t> row;
                row.push_back(kv.first);
                const AggRes &r = kv.second;
                switch (agg.op) {
                    case AggregateOp::COUNT: row.push_back(r.count); break;
                    case AggregateOp::SUM:   row.push_back(r.sum);   break;
                    case AggregateOp::MIN:   row.push_back(r.minv);  break;
                    case AggregateOp::MAX:   row.push_back(r.maxv);  break;
                    case AggregateOp::AVG: {
                        double d = (r.count == 0) ? 0.0 : double(r.sum) / r.count;
                        row.push_back(d);
                        break;
                    }
                }
                out.insertTuple(Tuple(row));
            }
        } else if (gtype == type_t::CHAR) {
            std::unordered_map<std::string, AggRes> m;
            for (Iterator it = in.begin(); it != in.end(); ++it) {
                Tuple t = *it;
                std::string gval = std::get<std::string>(t.get_field(groupIdx));
                int av = std::get<int>(t.get_field(aggIdx));
                auto &r = m[gval];
                if (r.count == 0) {
                    r.minv = av;
                    r.maxv = av;
                } else {
                    if (av < r.minv) r.minv = av;
                    if (av > r.maxv) r.maxv = av;
                }
                r.sum += av;
                r.count++;
            }
            for (auto &kv : m) {
                std::vector<field_t> row;
                row.push_back(kv.first);
                const AggRes &r = kv.second;
                switch (agg.op) {
                    case AggregateOp::COUNT: row.push_back(r.count); break;
                    case AggregateOp::SUM:   row.push_back(r.sum);   break;
                    case AggregateOp::MIN:   row.push_back(r.minv);  break;
                    case AggregateOp::MAX:   row.push_back(r.maxv);  break;
                    case AggregateOp::AVG: {
                        double d = (r.count == 0) ? 0.0 : double(r.sum) / r.count;
                        row.push_back(d);
                        break;
                    }
                }
                out.insertTuple(Tuple(row));
            }
        } else {
            // handle double if needed
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
                case PredicateOp::LT: match = (lv < rv);  break;
                case PredicateOp::LE: match = (lv <= rv); break;
                case PredicateOp::GT: match = (lv > rv);  break;
                case PredicateOp::GE: match = (lv >= rv); break;
            }
            if (match) {
                std::vector<field_t> merged;
                for (size_t i = 0; i < lt.size(); i++) merged.push_back(lt.get_field(i));
                for (size_t i = 0; i < rt.size(); i++) {
                    if (pred.op == PredicateOp::EQ && i == ri) continue;
                    merged.push_back(rt.get_field(i));
                }
                out.insertTuple(Tuple(merged));
            }
        }
    }
}
