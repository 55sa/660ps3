## **Overview of Query.cpp**  
The file provides functions for:

1. **Filtering:** Selecting tuples based on specific conditions.  
2. **Projection:** Selecting specific fields from tuples.  
3. **Aggregation:** Performing calculations (COUNT, SUM, MIN, MAX, AVG) over groups of tuples.  
4. **Joining:** Combining tuples from two different database files based on a join condition.

## **Function Explanations**

### **1\. evalFilter (Static Helper Function)**

* **Purpose:** Evaluates a single filter predicate against a given tuple. It checks if a specific field in the tuple satisfies the condition defined by the predicate.  
* **Parameters:**  
  * t (const Tuple&): The input tuple to evaluate.  
  * p (const FilterPredicate&): The filter condition (field name, operator, value).  
  * td (const TupleDesc&): The tuple descriptor, used to find the index of the field name.  
* **Logic:**  
  1. Finds the index of the field specified in the predicate using the TupleDesc.  
  2. Retrieves the integer value of that field from the tuple (tv).  
  3. Retrieves the integer value from the predicate (pv).  
  4. Uses a switch statement based on the predicate's operator (p.op) to compare tv and pv.  
  5. Returns true if the condition is met, false otherwise.  

### **2\. db::projection**


* **Parameters:**  
  * in (const DbFile&): The input database file.  
  * out (DbFile&): The output database file where projected tuples are stored.  
  * fields (const std::vector\<std::string\>&): A list of field names to include in the projection.  
* **Logic:**  
  1. Iterates through each Tuple (t) in the input file (in).  
  2. For each tuple, creates a new vector (vals) to hold the projected field values.  
  3. Iterates through the requested fields.  
  4. For each field name, finds its index in the input tuple's descriptor (in.getTupleDesc()).  
  5. Retrieves the corresponding field value from the tuple t and adds it to vals.  
  6. Creates a new Tuple from vals and inserts it into the output file (out).

### **3\. db::filter**

* **Parameters:**  
  * in (const DbFile&): The input database file.  
  * out (DbFile&): The output database file where filtered tuples are stored.  
  * preds (const std::vector\<FilterPredicate\>&): A list of filter predicates to apply.  
* **Logic:**  
  1. Gets the TupleDesc from the input file.  
  2. Iterates through each Tuple (t) in the input file (in).  
  3. For each tuple, assumes it passes (pass \= true).  
  4. Iterates through each FilterPredicate (p) in preds.  
  5. Calls evalFilter(t, p, td) to check if the tuple satisfies the current predicate.  
  6. If evalFilter returns false, sets pass to false and breaks the inner loop (no need to check other predicates).  
  7. If, after checking all predicates, pass is still true, inserts the original tuple t into the output file (out).

### **4\. db::aggregate**

* **Parameters:**  
  * in (const DbFile&): The input database file.  
  * out (DbFile&): The output database file where aggregate results are stored.  
  * agg (const Aggregate&): An object specifying the aggregation operation (field to aggregate, operation type, optional grouping field).  
* **Logic:**  
  * **No Grouping:**  
    1. Initializes an AggRes struct to store running sum, min, max, and count.  
    2. Iterates through all tuples in the input file.  
    3. Extracts the integer value (v) of the field to be aggregated.  
    4. Updates the sum, count, minv, and maxv in the AggRes struct.  
    5. After iterating, calculates the final aggregate result based on agg.op (COUNT, SUM, MIN, MAX, AVG).  
    6. Creates a single output tuple containing the result and inserts it into out.  
  * **With Grouping:**  
    1. Determines the index and type of the grouping field.  
    2. Uses an std::unordered\_map (m) to store AggRes structs, keyed by the group value (either int or std::string).  
    3. Iterates through all tuples in the input file.  
    4. Extracts the grouping value (gv or gval) and the aggregation value (av).  
    5. Finds or creates the AggRes entry for the current group in the map (m\[group\_value\]).  
    6. Updates the sum, count, minv, and maxv for that specific group's AggRes.  
    7. After iterating through all input tuples, iterates through the key-value pairs in the map m.  
    8. For each group, calculates the final aggregate result based on agg.op.  
    9. Creates an output tuple containing the group value and the calculated aggregate result.  
    10. Inserts this tuple into the output file (out).  

### **5\. db::join**

* **Parameters:**  
  * left (const DbFile&): The left input database file.  
  * right (const DbFile&): The right input database file.  
  * out (DbFile&): The output database file where joined tuples are stored.  
  * pred (const JoinPredicate&): The join condition (left field, right field, comparison operator).  
* **Logic:**  
  1. Gets the TupleDesc for both input files.  
  2. Finds the indices (li, ri) of the join fields in their respective tuple descriptors.  
  3. Uses nested loops to iterate through every pair of tuples (lt from left, rt from right).  
  4. Extracts the integer join field values (lv, rv) from the current pair of tuples.  
  5. Evaluates the join predicate's operator (pred.op) to compare lv and rv.  
  6. If the condition (match) is true:  
     * Creates a new vector (merged) to store the fields of the combined tuple.  
     * Copies all fields from the left tuple (lt) into merged.  
     * Copies all fields from the right tuple (rt) into merged, (PredicateOp::EQ) to avoid duplication.  
     * Creates a new Tuple from merged and inserts it into the output file (out).  