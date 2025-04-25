## Q1

**Modify the Filter Operator:**
Enhance the filter operator to handle complex predicates directly:
```
filter(input_file, output_file, lambda tuple: predicate1(tuple) or predicate2(tuple))
```
This evaluates the combined predicate predicate1 OR predicate2 in a single pass over input_file, writing qualifying tuples directly to output_file.

## Q2

The aggregate operation supports grouping by a single field.

### Extending Grouping to Multiple Fields:

To group by multiple fields, use a composite key formed by combining the values of all specified grouping fields.

#### Implementation:
1. For each tuple, extract values from grouping columns.
2. Combine these into a composite key structure.
3. Use this composite key for grouping (e.g., as a hash key or sort key).
4. Update aggregate functions associated with that specific composite key.
5. The output contains one row per unique combination of grouping values, plus the aggregates.

### Implementing the HAVING Clause:

The HAVING clause filters groups after aggregation, based on aggregate results.

#### Implementation:
1. Perform the aggregate operation, with single or multi-field grouping to produce an intermediate result set of groups and their aggregate values.
2. Apply a second filtering step to this intermediate result, evaluating the HAVING predicate against each group row.
3. Output only the groups that satisfy the HAVING predicate.

## Q3

### Complexity:
- **Block/Page-based NLJ:** Read blocks of the outer relation R (size M pages), and for each block of R, read all blocks of the inner relation S (size N pages).
- **IO Complexity:** Approximately M+(B<sub>R</sub>×N) or often simplified to O(M×N) page reads, where B<sub>R</sub> is the number of blocks/pages in R. This is computationally expensive for large tables.
- **CPU Complexity:** O(|R|×|S|) tuple comparisons in the worst case.

### Improvement:

**Hash Join:** Generally much faster for equi-joins if memory is sufficient.
1. **Build Phase:** Scan the smaller relation, build an in-memory hash table on the join attribute. IO Cost: M.
2. **Probe Phase:** Scan the larger relation, hash each tuple, and probe the hash table for matches. IO Cost: N.
3. **Total IO Cost:** M+N. Complexity: O(M+N) IOs . Significantly better. Handles memory limitations with variants like Grace or Hybrid Hash Join.

## Q4

**Using Selection Cardinality:** Knowing only the selection predicate's cardinality is insufficient to accurately estimate IO cost. It doesn't tell you how many disk pages must be read. 1000 tuples could be on 20 pages or 1000 pages.

### Other Factors Needed for IO Cost Estimation:

1. **Access Method:**
   - Full Table Scan: Reads all table pages. IO Cost = Total pages.
   - Index Scan: Uses an index to find tuples. Cost depends on the index.

2. **Index Type and Structure:** (If using an index)
   - B+ Tree: IO cost includes tree traversal (height), reading leaf pages, and reading data pages.
   - Hash Index: IO cost includes bucket access and reading data pages (for equality).

3. **Clustering:**
   - Clustered Index: Data is physically ordered with the index. Matching tuples are close together, minimizing data page reads.
   - Unclustered Index: Data is stored elsewhere. Fetching N tuples can require up to N data page reads (worst case).

4. **Data Distribution / Skew:** Affects the accuracy of estimates assuming uniform distribution.

5. **Buffering / Caching:** Buffer pool contents can reduce physical IO reads. Cost models often estimate physical IO.

6. **Predicate Type:** Equality, range, etc., affect how efficiently an index can be used.

## Q5

Histograms approximate value distributions. To estimate |R⋈<sub>R.A=S.B</sub>S| using histograms on R.A and S.B:

1. **Align Histograms:** Consider pairs of buckets (one from R's histogram, one from S's) that cover overlapping value ranges.

2. **Estimate Matches per Overlapping Bucket Pair:** For an overlapping pair (bucket i from R, bucket j from S):
   - Estimate the number of tuples from R (f<sub>Ri</sub>′) and S (f<sub>Sj</sub>′) falling within the specific overlap range, often assuming uniform distribution within the original buckets.
   - Estimate the number of distinct values within the overlap range (v<sub>overlap</sub>). This is a complex part of estimation, relying on assumptions about value distribution.
   - Calculate the contribution to join cardinality from this pair, often using a formula derived from assumptions of uniformity within the overlap:
     
     Matches<sub>ij</sub> = (f<sub>Ri</sub>′ × f<sub>Sj</sub>′) / v<sub>overlap</sub>
     
     This formula estimates matches by considering how many tuples from each relation likely share each distinct value within the overlap.

3. **Sum Contributions:** Sum the estimated matches over all pairs of overlapping buckets:
   
   |R⋈S| ≈ ∑<sub>i,j s.t. buckets overlap</sub> Matches<sub>ij</sub>

This histogram-based method accounts for data skew better than simpler formulas that assume uniform distribution across the entire column.

## Q6

### Scenario 1: BTreeFile

1. **Index Traversal:** Read root, 1 internal page to find the first data/leaf page. Cost = 2 IOs.
2. **Data/Leaf Page Access:** 1000 tuples fit on ⌈1000/50⌉=20 pages. These pages are contiguous. Cost = 20 IOs.
3. **Total IO:** 2+20=22 IOs.

### Scenario 2: HeapFile
1. **Access Method:** Must perform a Full Table Scan.
2. **Total IO:** Read every page. Cost = 150,000 page reads.

### Comparison:
- HeapFile: 150,000 IOs
- BTreeFile: 22 IOs