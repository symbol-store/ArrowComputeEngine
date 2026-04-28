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

  (test "Project: column alias with (as expr name)"
        '(Table (revenue 90.0 180.0))
        (boss-eval
          (Project (Table (price 100 200) (discount 0.1 0.1))
                   (as (multiply price (subtract 1.0 discount)) revenue))))

  (test "Project: mix of plain columns and aliases"
        '(Table (price 100 200) (revenue 90.0 180.0))
        (boss-eval
          (Project (Table (price 100 200) (discount 0.1 0.1))
                   price
                   (as (multiply price (subtract 1.0 discount)) revenue))))

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

  (test "GroupBy: count_all global"
        '(Table (|count_all()| 3))
        (boss-eval (GroupBy (Table (A 1 2 3)) (count_all))))

  (test "GroupBy: count_all with key"
        '(Table (grp 1 2) (|count_all()| 2 2))
        (boss-eval
          (OrderBy
            (GroupBy (Table (grp 1 1 2 2) (val 1 2 3 4)) (count_all) grp)
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

  ;;; Join

  (test "Join: inner — matching rows only (key columns get _l/_r suffix)"
        '(Table (id_l 1 2) (val 10 20) (id_r 1 2) (score 100 200))
        (boss-eval
          (Join (Table (id 1 2 3) (val 10 20 30)) (keys id)
                (Table (id 1 2 4) (score 100 200 400)) (keys id))))

  (test "LeftJoin: keeps all left rows, NULL for unmatched right"
        '(Table (id_l 1 2 3) (val 10 20 30) (id_r 1 2 NULL) (score 100 200 NULL))
        (boss-eval
          (LeftJoin (Table (id 1 2 3) (val 10 20 30)) (keys id)
                    (Table (id 1 2 4) (score 100 200 400)) (keys id))))

  (test "AntiJoin: left rows with no match in right"
        '(Table (id 3) (val 30))
        (boss-eval
          (AntiJoin (Table (id 1 2 3) (val 10 20 30)) (keys id)
                    (Table (id 1 2 4) (score 100 200 400)) (keys id))))

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

  ;; Q1: sum + count_all grouped by two keys, ordered
  ;; rows: (q=5,rf=1,ls=1),(q=5,rf=1,ls=1),(q=20,rf=1,ls=2),(q=15,rf=2,ls=1)
  ;; groups (rf,ls): (1,1)→sum=10,count=2  (1,2)→sum=20,count=1  (2,1)→sum=15,count=1
  (test "Q1: sum + count_all by two keys"
        '(Table (returnflag 1 1 2) (linestatus 1 2 1) (|sum(quantity)| 10 20 15) (|count_all()| 2 1 1))
        (boss-eval
          (OrderBy
            (GroupBy
              (Table (quantity 5 5 20 15) (returnflag 1 1 1 2) (linestatus 1 1 2 1))
              (sum quantity) (count_all) returnflag linestatus)
            (keys returnflag linestatus))))

  ;; Q3: join orders + lineitems (distinct key names → no _l/_r suffix), group by orderkey, order by revenue desc
  ;; join: o_ok=1→ep 100+200=300, o_ok=2→ep=200, o_ok=3→ep=50
  (test "Q3: join + groupby orderkey + order by revenue desc"
        '(Table (o_orderkey 1 2 3) (|sum(extendedprice)| 300 200 50))
        (boss-eval
          (OrderBy
            (GroupBy
              (Join (Table (o_orderkey 1 2 3) (custkey 1 1 2)) (keys o_orderkey)
                    (Table (l_orderkey 1 1 2 3) (extendedprice 100 200 200 50)) (keys l_orderkey))
              (sum extendedprice) o_orderkey)
            (keys (desc |sum(extendedprice)|)))))

  ;; Q10: multi-key groupby ordered by sum desc
  ;; groups: (cust=1,name=1)→100, (cust=2,name=2)→50, (cust=3,name=3)→300
  (test "Q10: multi-key groupby + order by sum desc"
        '(Table (custkey 3 1 2) (name 3 1 2) (|sum(amount)| 300 100 50))
        (boss-eval
          (OrderBy
            (GroupBy
              (Table (custkey 1 1 2 3) (name 1 1 2 3) (amount 60 40 50 300))
              (sum amount) custkey name)
            (keys (desc |sum(amount)|)))))

  ;; Q13: LeftJoin then count orders per customer; customer 3 has no orders → count=0
  (test "Q13: LeftJoin + count(nullable) per customer"
        '(Table (custkey_l 1 2 3) (|count(orderkey)| 2 1 0))
        (boss-eval
          (OrderBy
            (GroupBy
              (LeftJoin
                (Table (custkey 1 2 3)) (keys custkey)
                (Table (custkey 1 1 2) (orderkey 10 20 30)) (keys custkey))
              (count orderkey) custkey_l)
            (keys custkey_l))))

  ;; Q16: AntiJoin to exclude disqualified suppliers, count_all by brand, order desc
  ;; parts with suppkey=3 removed; brand=1 has 2 left, brand=2 has 1 left
  (test "Q16: AntiJoin + count_all by brand ordered desc"
        '(Table (brand 1 2) (|count_all()| 2 1))
        (boss-eval
          (OrderBy
            (GroupBy
              (AntiJoin
                (Table (suppkey 1 2 3 3 4) (brand 1 1 1 2 2)) (keys suppkey)
                (Table (suppkey 3)) (keys suppkey))
              (count_all) brand)
            (keys (desc |count_all()|)))))

  ;; Q18: HAVING via Filter-after-GroupBy stored with Name; semi-join lineitems to qualifying orders
  ;; heavy orders: orderkey=1 (sum=15 > 10); join filters lineitems; sum quantity per customer
  (test "Q18: HAVING subquery + semi-join + groupby + order desc"
        '(Table (custkey 1) (|sum(quantity)| 15))
        (begin
          (boss-eval (Name
            (Filter
              (GroupBy (Table (orderkey 1 1 2) (quantity 8 7 3)) (sum quantity) orderkey)
              (Greater |sum(quantity)| 10))
            q18_heavy))
          (boss-eval
            (OrderBy
              (GroupBy
                (Join (Table (orderkey 1 1 2) (custkey 1 1 2) (quantity 8 7 3)) (keys orderkey)
                      (ByName q18_heavy) (keys orderkey))
                (sum quantity) custkey)
              (keys (desc |sum(quantity)|))))))

  ;; Q21: AntiJoin to remove problem suppliers, count_all per region, order desc
  ;; suppkey=2 excluded; region=1 has sk=1,3 left (2), region=2 has sk=4,5,6 left (3)
  (test "Q21: AntiJoin + count_all by region ordered desc"
        '(Table (region 2 1) (|count_all()| 3 2))
        (boss-eval
          (OrderBy
            (GroupBy
              (AntiJoin
                (Table (suppkey 1 2 3 4 5 6) (region 1 1 1 2 2 2)) (keys suppkey)
                (Table (suppkey 2)) (keys suppkey))
              (count_all) region)
            (keys (desc |count_all()|)))))

  ;; Q4: semi-join via Name/ByName — count orders that have a qualifying lineitem
  ;; orders (ok=1,p=1),(ok=2,p=2),(ok=3,p=2); qualifying: ok=1,ok=2
  ;; after join: ok=1(p=1), ok=2(p=2) → count_all by priority: p=1→1, p=2→1
  (test "Q4: semi-join (Name/ByName) + count_all by priority"
        '(Table (priority 1 2) (|count_all()| 1 1))
        (begin
          (boss-eval (Name (Table (orderkey 1 2)) q4_qualifying))
          (boss-eval
            (OrderBy
              (GroupBy
                (Join (Table (orderkey 1 2 3) (priority 1 2 2)) (keys orderkey)
                      (ByName q4_qualifying) (keys orderkey))
                (count_all) priority)
              (keys priority)))))

  ;; Q5: multi-key groupby + aliased aggregate column
  ;; groups (nation=1,yr=2020)→100.0, (nation=1,yr=2021)→200.0, (nation=2,yr=2020)→150.0
  (test "Q5: multi-key groupby + aliased aggregate"
        '(Table (nation 1 1 2) (yr 2020 2021 2020) (revenue 100.0 200.0 150.0))
        (boss-eval
          (OrderBy
            (Project
              (GroupBy
                (Table (nation 1 1 2) (yr 2020 2021 2020) (extendedprice 100.0 200.0 150.0))
                (sum extendedprice) nation yr)
              nation yr (as |sum(extendedprice)| revenue))
            (keys nation yr))))

  ;; Q7: year extraction from unix timestamp in filter + project
  ;; 1592179200 = 2020-06-15 UTC; 1623715200 = 2021-06-15 UTC → keep only 2020 row
  (test "Q7: year extraction from unix timestamp"
        '(Table (shipyear 2020))
        (boss-eval
          (Project
            (Filter
              (Table (l_shipdate 1592179200 1623715200))
              (Equal (year (timestamp l_shipdate)) 2020))
            (as (year (timestamp l_shipdate)) shipyear))))

  ;; Q8: if_else in project + column aliasing
  ;; type="A" rows contribute extendedprice to revenue, others contribute 0.0
  (test "Q8: if_else in project + column aliasing"
        '(Table (nation 1 2) (revenue 100.0 0.0))
        (boss-eval
          (OrderBy
            (Project
              (Table (nation 1 2) (extendedprice 100.0 50.0) (type "A" "B"))
              nation (as (if_else (Equal type "A") extendedprice 0.0) revenue))
            (keys nation))))

  ;; Q9: extract year from unix timestamp, then groupby year
  ;; 694224000 = 1992-01-01 UTC; 694224001 = same year; 725846400 = 1993-01-01 UTC
  ;; project year first → yr column; groupby yr; sum profit
  (test "Q9: project year from timestamp + groupby + sum"
        '(Table (yr 1992 1993) (|sum(profit)| 100.0 200.0))
        (boss-eval
          (OrderBy
            (GroupBy
              (Project
                (Table (shipdate 694224000 694224001 725846400) (profit 60.0 40.0 200.0))
                (as (year (timestamp shipdate)) yr) profit)
              (sum profit) yr)
            (keys yr))))

  ;; Q11: HAVING via Filter-after-GroupBy + result stored with Name/ByName
  ;; partkey=1: sum=600.0 > 400.0 → kept; partkey=2: sum=100.0 → filtered out
  (test "Q11: groupby + HAVING filter + Name/ByName retrieval"
        '(Table (partkey 1) (|sum(value)| 600.0))
        (begin
          (boss-eval (Name
            (Filter
              (GroupBy (Table (partkey 1 1 2) (value 400.0 200.0 100.0)) (sum value) partkey)
              (Greater |sum(value)| 400.0))
            q11_result))
          (boss-eval (ByName q11_result))))

  ;; Q12: groupby shipmode + aliased count column
  ;; mode=1 has 3 rows, mode=2 has 1 row
  (test "Q12: groupby shipmode + aliased count_all"
        '(Table (shipmode 1 2) (high_count 3 1))
        (boss-eval
          (OrderBy
            (Project
              (GroupBy
                (Table (shipmode 1 1 1 2))
                (count_all) shipmode)
              shipmode (as |count_all()| high_count))
            (keys shipmode))))

  ;; Q14: multiple aggregates with aliased output columns (no groupby keys)
  ;; sum(extendedprice)=500.0, sum(promo_price)=200.0; aliased to total_revenue / promo_revenue
  (test "Q14: multi-aggregate + aliased output columns"
        '(Table (promo_revenue 200.0) (total_revenue 500.0))
        (boss-eval
          (Project
            (GroupBy
              (Table (extendedprice 100.0 200.0 200.0) (promo_price 0.0 200.0 0.0))
              (sum extendedprice) (sum promo_price))
            (as |sum(promo_price)| promo_revenue)
            (as |sum(extendedprice)| total_revenue))))

  ;; Q19: complex nested AND/OR filter
  ;; keep: (q<24 AND price>90) OR (q>25 AND price<200)
  ;; row1: q=17<24 AND price=100>90 → TRUE; row2: q=36, 200<200 FALSE; row3: 80>90 FALSE; row4: q=28>25 AND 150<200 → TRUE
  (test "Q19: nested AND/OR filter predicate"
        '(Table (quantity 17 28) (price 100.0 150.0))
        (boss-eval
          (Filter
            (Table (quantity 17 36 8 28) (price 100.0 200.0 80.0 150.0))
            (Or (And (Less quantity 24) (Greater price 90.0))
                (And (Greater quantity 25) (Less price 200.0))))))

  ;; Q20: non-correlated subquery via Name/ByName — join parts to qualifying supply subset
  ;; partkey=1: sum(qty)=15 > 5 → qualifying; partkey=2: sum=3 → not qualifying
  (test "Q20: Name/ByName non-correlated subquery"
        '(Table (p_partkey 1))
        (begin
          (boss-eval (Name
            (Filter
              (GroupBy (Table (partkey 1 1 2) (qty 8 7 3)) (sum qty) partkey)
              (Greater |sum(qty)| 5))
            q20_qualifying))
          (boss-eval
            (Project
              (Join (Table (p_partkey 1 2) (name "part1" "part2")) (keys p_partkey)
                    (ByName q20_qualifying) (keys partkey))
              p_partkey))))

)

(test-exit)
