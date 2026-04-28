(import (scheme base)
        (chibi test)
        (BOSS))

(boss-eval (SetDefaultEnginePipeline "Build/libArrowComputeEngine.so"))

(test-group "Arrow Compute Operators"

  ;;; Table — inline data ingestion

  (test "Table: single integer column"
        '(Table (A 1 2 3))
        (boss-eval (Table (A 1 2 3))))

  (test "Table: multiple columns"
        '(Table (A 1 2) (B 3 4))
        (boss-eval (Table (A 1 2) (B 3 4))))

  (test "Table: column with NULLs"
        '(Table (A 1 NULL 3))
        (boss-eval (Table (A 1 NULL 3))))

  ;;; Filter

  (test "Filter: greater"
        '(Table (A 5))
        (boss-eval (Filter (Table (A 5 1 3)) (Greater A 4))))

  (test "Filter: less"
        '(Table (A 1 3))
        (boss-eval (Filter (Table (A 5 1 3)) (Less A 4))))

  (test "Filter: equal"
        '(Table (A 2))
        (boss-eval (Filter (Table (A 1 2 3)) (Equal A 2))))

  (test "Filter: multi-column — keeps all columns"
        '(Table (A 2) (B 20))
        (boss-eval (Filter (Table (A 1 2 3) (B 10 20 30)) (Equal A 2))))

  ;;; Project

  (test "Project: select one column"
        '(Table (A 1 2 3))
        (boss-eval (Project (Table (A 1 2 3) (B 4 5 6)) A)))

  (test "Project: select multiple columns"
        '(Table (A 1 2) (B 3 4))
        (boss-eval (Project (Table (A 1 2) (B 3 4) (C 5 6)) A B)))

  ;;; Slice

  (test "Slice: offset 0"
        '(Table (A 1 2))
        (boss-eval (Slice (Table (A 1 2 3 4)) 0 2)))

  (test "Slice: offset mid"
        '(Table (A 2 3))
        (boss-eval (Slice (Table (A 1 2 3 4)) 1 2)))

  ;;; OrderBy

  (test "OrderBy: ascending"
        '(Table (A 1 2 3))
        (boss-eval (OrderBy (Table (A 3 1 2)) (keys A))))

  (test "OrderBy: multi-key"
        '(Table (A 1 1 2) (B 1 2 1))
        (boss-eval (OrderBy (Table (A 2 1 1) (B 1 2 1)) (keys A B))))

  (test "OrderBy: descending"
        '(Table (A 3 2 1))
        (boss-eval (OrderBy (Table (A 1 3 2)) (keys (desc A)))))

  (test "OrderBy: mixed asc/desc"
        '(Table (A 1 1 2) (B 2 1 1))
        (boss-eval (OrderBy (Table (A 2 1 1) (B 1 2 1)) (keys A (desc B)))))

  ;;; GroupBy

  (test "GroupBy: global sum"
        '(Table (|sum(A)| 6))
        (boss-eval (GroupBy (Table (A 1 2 3)) (sum A))))

  (test "GroupBy: global count"
        '(Table (|count(A)| 3))
        (boss-eval (GroupBy (Table (A 1 2 3)) (count A))))

  (test "GroupBy: global mean"
        '(Table (|mean(A)| 2.0))
        (boss-eval (GroupBy (Table (A 1 2 3)) (mean A))))

  (test "GroupBy: two keys (Q1-style returnflag + linestatus)"
        '(Table (returnflag 1 1 2 2) (linestatus 1 2 1 2) (|sum(quantity)| 17 36 8 28))
        (boss-eval
          (OrderBy
            (GroupBy (Table (quantity 17 36 8 28) (returnflag 1 1 2 2) (linestatus 1 2 1 2))
                     (sum quantity)
                     returnflag linestatus)
            (keys returnflag linestatus))))

  (test "GroupBy: multiple aggregates global"
        '(Table (|sum(A)| 6) (|mean(A)| 2.0))
        (boss-eval (GroupBy (Table (A 1 2 3)) (sum A) (mean A))))

  (test "GroupBy: multiple aggregates with key"
        '(Table (grp 1 2) (|sum(val)| 3 7) (|count(val)| 2 2))
        (boss-eval
          (OrderBy
            (GroupBy (Table (grp 1 1 2 2) (val 1 2 3 4))
                     (sum val) (count val)
                     grp)
            (keys grp))))

  ;;; Materialize

  (test "Materialize: preserves data"
        '(Table (A 1 2 3))
        (boss-eval (Materialize (Table (A 1 2 3)))))

  ;;; ToStatus

  (test "ToStatus: returns OK"
        "OK"
        (boss-eval (ToStatus (Table (A 1 2 3)))))

  ;;; Cumulate

  (test "Cumulate: running sum appends column"
        '(Table (A 1 2 3) (|sum(A)| 1 3 6))
        (boss-eval (Cumulate (Table (A 1 2 3)) (sum A))))

  ;;; Pairwise
  ;; pairwise_diff: out[i] = in[i] - in[i-lag]; first lag elements are NULL

  (test "Pairwise: diff with lag 1"
        '(Table (A 1 2 4 8) (diff NULL 1 2 4))
        (boss-eval (Pairwise (Table (A 1 2 4 8)) diff A 1)))

  (test "Pairwise: diff with lag 2"
        '(Table (A 1 2 4 8) (diff NULL NULL 3 6))
        (boss-eval (Pairwise (Table (A 1 2 4 8)) diff A 2)))

  ;;; Name / ByName

  (test "Name and ByName: round-trip"
        '(Table (A 1 2 3))
        (begin
          (boss-eval (Name (Table (A 1 2 3)) testTable))
          (boss-eval (ByName testTable))))

  ;;; Composition

  (test "Filter then OrderBy"
        '(Table (A 1 2 3))
        (boss-eval (OrderBy (Filter (Table (A 3 5 1 2)) (Less A 4)) (keys A))))

  (test "Project then Filter"
        '(Table (A 2 3))
        (boss-eval (Filter (Project (Table (A 1 2 3) (B 4 5 6)) A) (Greater A 1))))

)

(test-group "TPC-H inspired"

  ;; Q1-like: aggregate quantity by returnflag and linestatus, ordered for determinism
  (test "Q1-like: sum quantity by returnflag and linestatus"
        '(Table (returnflag 1 1 2 2) (linestatus 1 2 1 2) (|sum(quantity)| 17 36 8 28))
        (boss-eval
          (OrderBy
            (GroupBy (Table (quantity 17 36 8 28) (returnflag 1 1 2 2) (linestatus 1 2 1 2))
                     (sum quantity)
                     returnflag linestatus)
            (keys returnflag linestatus))))

  ;; Q6-like: sum revenue for items with quantity below threshold (17, 8 qualify)
  (test "Q6-like: filtered revenue sum"
        '(Table (|sum(extendedprice)| 180))
        (boss-eval
          (GroupBy
            (Filter (Table (quantity 17 36 8 28) (extendedprice 100 200 80 150))
                    (Less quantity 24))
            (sum extendedprice))))

  ;; Q3-like: join orders with lineitems for a given customer, sum revenue
  ;; custkey=1 has orderkeys 1 and 2; matching lineitems give prices 100+200+150=450
  (test "Q3-like: join + filtered aggregate"
        '(Table (|sum(extendedprice)| 450))
        (boss-eval
          (GroupBy
            (Join
              (Filter (Table (orderkey 1 2 3) (custkey 1 1 2)) (Equal custkey 1))
              (keys orderkey)
              (Table (orderkey 1 1 2 3) (extendedprice 100 200 150 300))
              (keys orderkey))
            (sum extendedprice))))

  ;; Q2-like: find the two cheapest parts using OrderBy + Slice
  (test "Q2-like: two cheapest parts"
        '(Table (partkey 2 4) (price 80 120))
        (boss-eval
          (Slice
            (OrderBy (Table (partkey 1 2 3 4) (price 150 80 200 120)) (keys price))
            0 2)))

  ;; Covid/TPC-H hybrid: running total then lag-1 diff reproduces the hotspot query pattern
  ;; sum(sales)[i] = cumulative sum; smoothed[i] = sum(sales)[i] - sum(sales)[i-1] = sales[i]
  (test "Cumulate then Pairwise: running total with lag-1 smoothing"
        '(Table (sales 10 20 15 30 5) (|sum(sales)| 10 30 45 75 80) (smoothed NULL 20 15 30 5))
        (boss-eval
          (Pairwise
            (Cumulate (Table (sales 10 20 15 30 5)) (sum sales))
            smoothed |sum(sales)| 1)))

  ;; Multi-query reuse: store orders once, run two different aggregations
  (test "Named table: filter then aggregate"
        '(Table (|sum(quantity)| 15))
        (begin
          (boss-eval (Name (Table (orderkey 1 1 2) (quantity 5 10 5)) orders))
          (boss-eval (GroupBy (Filter (ByName orders) (Equal orderkey 1)) (sum quantity)))))

)

(test-exit)
