;;; ==========================================================================
;;; TPC-H Query Plans — shared between tpch-bench.scm and repl-tests.scm
;;;
;;; Each query is a define-syntax macro.  Table arguments accept any BOSS
;;; table expression (e.g. (ByName table) for the benchmark, or an inline
;;; (Table ...) for the unit tests).  Date-bound arguments for the benchmark
;;; are days since epoch (matching Arrow's date32[day] type); use date->days
;;; to convert "YYYY-MM-DD" strings.  Inline test data uses raw integer
;;; unix-second timestamps.
;;; ==========================================================================

(define (date->days s)
  ;; Convert "YYYY-MM-DD" to a (Date n) expression that Arrow casts to date32[day].
  ;; make-tm args: sec min hour day month-0based year-since-1900 isdst
  (let ((year  (string->number (substring s 0 4)))
        (month (string->number (substring s 5 7)))
        (day   (string->number (substring s 8 10))))
    (list 'Date (exact (floor (/ (time->seconds (make-tm 0 0 0 day (- month 1) (- year 1900) 0))
                                 86400))))))

;;; ---------------------------------------------------------------------------
;;; Q1 — Pricing Summary Report
;;; Full 10-column TPC-H output: 2 group-by keys + 8 aggregates.
;;; Parameters: lineitem, shipdate-bound (inclusive upper bound)
;;; Test data: 3 rows pass filter (shipdate <= 904608000), last row filtered out.
;;;   l_tax=0.25 keeps charge arithmetic exact in IEEE 754.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q1
  (syntax-rules ()
    ((_ lineitem shipdate-bound)
     (boss-eval
       (OrderBy
         (Project
           (GroupBy
             (Project
               (Filter lineitem (LessEqual l_shipdate shipdate-bound))
               l_returnflag l_linestatus l_quantity l_extendedprice l_discount l_tax
               (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) disc_price)
               (As (Multiply (Multiply l_extendedprice (Subtract 1.0 l_discount))
                             (Add 1.0 l_tax)) charge))
             (Sum l_quantity) (Sum l_extendedprice) (Sum disc_price) (Sum charge)
             (Avg l_quantity) (Avg l_extendedprice) (Avg l_discount) (CountAll)
             l_returnflag l_linestatus)
           l_returnflag l_linestatus
           (As |sum(l_quantity)| sum_qty)
           (As |sum(l_extendedprice)| sum_base_price)
           (As |sum(disc_price)| sum_disc_price)
           (As |sum(charge)| sum_charge)
           (As |mean(l_quantity)| avg_qty)
           (As |mean(l_extendedprice)| avg_price)
           (As |mean(l_discount)| avg_disc)
           (As |count_all()| count_order))
         (keys l_returnflag l_linestatus))))))

;;; ---------------------------------------------------------------------------
;;; Q2 — Minimum Cost Supplier
;;; Standard 8-column output.
;;; Parameters: supplier, nation, region, partsupp, part
;;; Test data: partkey=5 size=15 type="15-BRASS" in EUROPE/GERMANY.
;;;   suppkey=1 (cost=90, acctbal=100) wins min-cost; suppkey=2 (cost=120) excluded.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q2
  (syntax-rules ()
    ((_ supplier nation region partsupp part)
     (begin
       (boss-eval (Name
         (Join
           (Join supplier (keys s_nationkey)
                 (Join nation (keys n_regionkey)
                       (Filter region (Equal r_name "EUROPE"))
                       (keys r_regionkey))
                 (keys n_nationkey))
           (keys s_suppkey)
           partsupp (keys ps_suppkey))
         q2_supp_europe))
       (boss-eval (Name
         (Project
           (GroupBy (ByName q2_supp_europe) (Min ps_supplycost) ps_partkey)
           ps_partkey (As |min(ps_supplycost)| min_supplycost))
         q2_min_cost))
       (boss-eval
         (Project
           (Slice
             (OrderBy
               (Join
                 (Join
                   (Filter part (And (Equal p_size 15) (Like p_type "%BRASS")))
                   (keys p_partkey)
                   (ByName q2_supp_europe) (keys ps_partkey))
                 (keys ps_partkey ps_supplycost)
                 (ByName q2_min_cost) (keys ps_partkey min_supplycost))
               (keys (Desc s_acctbal) n_name s_name p_partkey))
             0 100)
           s_acctbal s_name n_name p_partkey p_mfgr s_address s_phone s_comment))))))

;;; ---------------------------------------------------------------------------
;;; Q3 — Shipping Priority
;;; Parameters: customer, orders, lineitem, cutoff (same date for both filters)
;;; Test data: BUILDING customer (custkey=1); orders before 795225600 (1995-03-15);
;;;   lineitems shipping after cutoff.  Order 3 (orderdate=800000000) filtered out.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q3
  (syntax-rules ()
    ((_ customer orders lineitem cutoff)
     (boss-eval
       (Slice
         (OrderBy
           (Project
             (GroupBy
               (Join
                 (Join
                   (Filter customer (Equal c_mktsegment "BUILDING"))
                   (keys c_custkey)
                   (Filter orders (Less o_orderdate cutoff))
                   (keys o_custkey))
                 (keys o_orderkey)
                 (Filter lineitem (Greater l_shipdate cutoff))
                 (keys l_orderkey))
               (Sum l_extendedprice) l_orderkey o_orderdate o_shippriority)
             l_orderkey o_orderdate o_shippriority
             (As |sum(l_extendedprice)| revenue))
           (keys (Desc revenue) o_orderdate))
         0 10)))))

;;; ---------------------------------------------------------------------------
;;; Q4 — Order Priority Checking
;;; Parameters: lineitem, orders, orderdate-lo, orderdate-hi
;;; Test data: orderkeys 1+2 qualify (commitdate < receiptdate); orderkey=3 excluded
;;;   (commit > receipt); orderkey=4 order is outside the date range.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q4
  (syntax-rules ()
    ((_ lineitem orders orderdate-lo orderdate-hi)
     (begin
       (boss-eval (Name
         (GroupBy
           (Filter lineitem (Less l_commitdate l_receiptdate))
           (CountAll) l_orderkey)
         q4_qualifying))
       (boss-eval
         (OrderBy
           (GroupBy
             (Join
               (Filter orders
                       (And (GreaterEqual o_orderdate orderdate-lo)
                            (Less o_orderdate orderdate-hi)))
               (keys o_orderkey)
               (ByName q4_qualifying) (keys l_orderkey))
             (CountAll) o_orderpriority)
           (keys o_orderpriority)))))))

;;; ---------------------------------------------------------------------------
;;; Q5 — Local Supplier Volume
;;; Restructured join (no post-join c_nationkey=s_nationkey filter):
;;; suppliers and customers are pre-matched by nationkey, then joined on both
;;; orderkey and suppkey.
;;; Parameters: supplier, nation, region, customer, orders, lineitem,
;;;             orderdate-lo, orderdate-hi
;;; Test data: ASIA region, CHINA nation (nationkey=5).  Supplier and customer
;;;   both in CHINA; one order in 1994 date range.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q5
  (syntax-rules ()
    ((_ supplier nation region customer orders lineitem orderdate-lo orderdate-hi)
     (begin
       ;; ASIA suppliers: (s_suppkey, s_nationkey, n_name)
       (boss-eval (Name
         (Project
           (Join
             supplier (keys s_nationkey)
             (Join
               nation (keys n_regionkey)
               (Filter region (Equal r_name "ASIA"))
               (keys r_regionkey))
             (keys n_nationkey))
           s_suppkey s_nationkey n_name)
         q5_supp_asia))
       ;; Customers matched to same nation as their supplier: (c_custkey, s_suppkey, n_name)
       (boss-eval (Name
         (Project
           (Join
             customer (keys c_nationkey)
             (ByName q5_supp_asia) (keys s_nationkey))
           c_custkey s_suppkey n_name)
         q5_cust_supp))
       ;; Orders in date range joined to customer-supplier pairs: (o_orderkey, s_suppkey, n_name)
       (boss-eval (Name
         (Project
           (Join
             (Filter orders (And (GreaterEqual o_orderdate orderdate-lo)
                                 (Less o_orderdate orderdate-hi)))
             (keys o_custkey)
             (ByName q5_cust_supp) (keys c_custkey))
           o_orderkey s_suppkey n_name)
         q5_ord_supp))
       ;; lineitem joined on (l_orderkey, l_suppkey) → revenue by nation
       (boss-eval
         (OrderBy
           (Project
             (GroupBy
               (Join
                 lineitem (keys l_orderkey l_suppkey)
                 (ByName q5_ord_supp) (keys o_orderkey s_suppkey))
               (Sum l_extendedprice) n_name)
             n_name (As |sum(l_extendedprice)| revenue))
           (keys (Desc revenue))))))))

;;; ---------------------------------------------------------------------------
;;; Q6 — Forecasting Revenue Change
;;; Parameters: lineitem, shipdate-lo, shipdate-hi
;;; Test data: filter shipdate∈[757382400,788918400), discount∈[0.05,0.07], qty<24.
;;;   Rows 2–4 filtered (qty>=24, date too early, qty<24 but same date as row 1).
;;;   revenue = extendedprice * discount.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q6
  (syntax-rules ()
    ((_ lineitem shipdate-lo shipdate-hi)
     (boss-eval
       (GroupBy
         (Project
           (Filter lineitem
                   (And (GreaterEqual l_shipdate shipdate-lo)
                        (And (Less l_shipdate shipdate-hi)
                             (And (GreaterEqual l_discount 0.05)
                                  (And (LessEqual l_discount 0.07)
                                       (Less l_quantity 24.0))))))
           (As (Multiply l_extendedprice l_discount) revenue))
         (Sum revenue))))))

;;; ---------------------------------------------------------------------------
;;; Q7 — Volume Shipping
;;; Parameters: nation, supplier, orders, customer, lineitem, shipdate-lo, shipdate-hi
;;; Test data: FRANCE→GERMANY pair (1996, volume=180) and GERMANY→FRANCE (1995, volume=100).
;;;   Date bounds: [788918400=1995-01-01, 851990400=1996-12-31].
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q7
  (syntax-rules ()
    ((_ nation supplier orders customer lineitem shipdate-lo shipdate-hi)
     (begin
       (boss-eval (Name
         (Project nation (As n_nationkey sn_nationkey) (As n_name supp_nation))
         q7_supp_nation))
       (boss-eval (Name
         (Project nation (As n_nationkey cn_nationkey) (As n_name cust_nation))
         q7_cust_nation))
       (boss-eval
         (OrderBy
           (GroupBy
             (Project
               (Filter
                 (Join
                   (Join
                     (Join
                       (Join
                         (Filter lineitem
                                 (And (GreaterEqual l_shipdate shipdate-lo)
                                      (LessEqual l_shipdate shipdate-hi)))
                         (keys l_suppkey)
                         supplier (keys s_suppkey))
                       (keys l_orderkey)
                       (Join orders (keys o_custkey) customer (keys c_custkey))
                       (keys o_orderkey))
                     (keys s_nationkey)
                     (ByName q7_supp_nation) (keys sn_nationkey))
                   (keys c_nationkey)
                   (ByName q7_cust_nation) (keys cn_nationkey))
                 (Or
                   (And (Equal supp_nation "FRANCE") (Equal cust_nation "GERMANY"))
                   (And (Equal supp_nation "GERMANY") (Equal cust_nation "FRANCE"))))
               supp_nation cust_nation
               (As (Year (Timestamp l_shipdate)) l_year)
               (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) volume))
             (Sum volume) supp_nation cust_nation l_year)
           (keys supp_nation cust_nation l_year)))))))

;;; ---------------------------------------------------------------------------
;;; Q8 — National Market Share
;;; Uses pre-filtered AMERICA customer list to avoid n_regionkey join issue.
;;; Parameters: nation, region, customer, supplier, part, orders, lineitem,
;;;             orderdate-lo, orderdate-hi
;;; Test data: BRAZIL supplier, USA customer in AMERICA.  One order in 1995,
;;;   part type "ECONOMY ANODIZED STEEL".  brazil_volume = volume = 200.0.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q8
  (syntax-rules ()
    ((_ nation region customer supplier part orders lineitem orderdate-lo orderdate-hi)
     (begin
       (boss-eval (Name
         (Project nation (As n_nationkey sn_nationkey) (As n_name supp_nation))
         q8_supp_nation))
       ;; Customers in AMERICA: customer ⋈ nation ⋈ region(AMERICA) → c_custkey only
       (boss-eval (Name
         (Project
           (Join
             (Join customer (keys c_nationkey) nation (keys n_nationkey))
             (keys n_regionkey)
             (Filter region (Equal r_name "AMERICA"))
             (keys r_regionkey))
           c_custkey)
         q8_cust_america))
       (boss-eval
         (OrderBy
           (GroupBy
             (Project
               (Join
                 (Join
                   (Join
                     (Join
                       (Join
                         (Filter orders
                                 (And (GreaterEqual o_orderdate orderdate-lo)
                                      (LessEqual o_orderdate orderdate-hi)))
                         (keys o_custkey)
                         (ByName q8_cust_america) (keys c_custkey))
                       (keys o_orderkey)
                       lineitem (keys l_orderkey))
                     (keys l_partkey)
                     (Filter part (Equal p_type "ECONOMY ANODIZED STEEL"))
                     (keys p_partkey))
                   (keys l_suppkey)
                   supplier (keys s_suppkey))
                 (keys s_nationkey)
                 (ByName q8_supp_nation) (keys sn_nationkey))
               (As (Year (Timestamp o_orderdate)) o_year)
               supp_nation
               (As (IfElse (Equal supp_nation "BRAZIL")
                           (Multiply l_extendedprice (Subtract 1.0 l_discount))
                           0.0)
                   brazil_volume)
               (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) volume))
             (Sum brazil_volume) (Sum volume) o_year)
           (keys o_year)))))))

;;; ---------------------------------------------------------------------------
;;; Q9 — Product Type Profit Measure
;;; Parameters: nation, supplier, part, lineitem, partsupp, orders
;;; Test data: CHINA+GERMANY nations; partkey=1 "dark green" passes Like "%green%",
;;;   partkey=2 "red" filtered.  Orders in 1992/1993.
;;;   profit = extprice*(1-disc) - supplycost*qty.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q9
  (syntax-rules ()
    ((_ nation supplier part lineitem partsupp orders)
     (begin
       (boss-eval (Name
         (Project nation n_nationkey (As n_name nation_name))
         q9_nation))
       (boss-eval
         (OrderBy
           (GroupBy
             (Project
               (Join
                 (Join
                   (Join
                     (Join
                       (Filter part (Like p_name "%green%"))
                       (keys p_partkey)
                       lineitem (keys l_partkey))
                     (keys l_suppkey l_partkey)
                     partsupp (keys ps_suppkey ps_partkey))
                   (keys l_orderkey)
                   orders (keys o_orderkey))
                 (keys l_suppkey)
                 (Join supplier (keys s_nationkey) (ByName q9_nation) (keys n_nationkey))
                 (keys s_suppkey))
               (As (Year (Timestamp o_orderdate)) o_year)
               nation_name
               (As (Subtract (Multiply l_extendedprice (Subtract 1.0 l_discount))
                             (Multiply ps_supplycost l_quantity))
                   profit))
             (Sum profit) nation_name o_year)
           (keys nation_name (Desc o_year))))))))

;;; ---------------------------------------------------------------------------
;;; Q10 — Returned Item Reporting
;;; Parameters: orders, customer, lineitem, nation, orderdate-lo, orderdate-hi
;;; Test data: Q4 1993 = [749433600, 757382400).  One order, two lineitems:
;;;   returnflag="R" (200.0) contributes to revenue; returnflag="N" filtered.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q10
  (syntax-rules ()
    ((_ orders customer lineitem nation orderdate-lo orderdate-hi)
     (boss-eval
       (Slice
         (OrderBy
           (Project
             (GroupBy
               (Join
                 (Join
                   (Join
                     (Filter orders
                             (And (GreaterEqual o_orderdate orderdate-lo)
                                  (Less o_orderdate orderdate-hi)))
                     (keys o_custkey)
                     customer (keys c_custkey))
                   (keys o_orderkey)
                   (Filter lineitem (Equal l_returnflag "R"))
                   (keys l_orderkey))
                 (keys c_nationkey)
                 nation (keys n_nationkey))
               (Sum l_extendedprice) c_custkey c_name c_acctbal c_phone n_name)
             c_custkey c_name c_acctbal c_phone n_name
             (As |sum(l_extendedprice)| revenue))
           (keys (Desc revenue)))
         0 20)))))

;;; ---------------------------------------------------------------------------
;;; Q11 — Important Stock Identification
;;; Parameters: partsupp, supplier, nation, threshold
;;; Test data: GERMANY supplier.  partkey=1: value=1600000 > threshold; partkey=2: value=10 filtered.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q11
  (syntax-rules ()
    ((_ partsupp supplier nation threshold)
     (begin
       (boss-eval (Name
         (Filter
           (GroupBy
             (Project
               (Join
                 partsupp (keys ps_suppkey)
                 (Join supplier (keys s_nationkey)
                       (Filter nation (Equal n_name "GERMANY"))
                       (keys n_nationkey))
                 (keys s_suppkey))
               ps_partkey
               (As (Multiply ps_supplycost ps_availqty) value))
             (Sum value) ps_partkey)
           (Greater |sum(value)| threshold))
         q11_result))
       (boss-eval
         (OrderBy (ByName q11_result) (keys (Desc |sum(value)|))))))))

;;; ---------------------------------------------------------------------------
;;; Q12 — Shipping Modes and Order Priority
;;; Parameters: orders, lineitem, receiptdate-lo, receiptdate-hi
;;; Test data: MAIL row (1-URGENT → high_flag=1); SHIP row (3-MEDIUM → low_flag=1).
;;;   receiptdate in [757382400, 788918400); shipdate < commitdate < receiptdate.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q12
  (syntax-rules ()
    ((_ orders lineitem receiptdate-lo receiptdate-hi)
     (boss-eval
       (OrderBy
         (GroupBy
           (Project
             (Join
               orders (keys o_orderkey)
               (Filter lineitem
                       (And (Or (Equal l_shipmode "MAIL") (Equal l_shipmode "SHIP"))
                            (And (Less l_shipdate l_commitdate)
                                 (And (Less l_commitdate l_receiptdate)
                                      (And (GreaterEqual l_receiptdate receiptdate-lo)
                                           (Less l_receiptdate receiptdate-hi))))))
               (keys l_orderkey))
             l_shipmode
             (As (IfElse (Or (Equal o_orderpriority "1-URGENT")
                             (Equal o_orderpriority "2-HIGH"))
                         1 0) high_flag)
             (As (IfElse (Or (Equal o_orderpriority "1-URGENT")
                             (Equal o_orderpriority "2-HIGH"))
                         0 1) low_flag))
           (Sum high_flag) (Sum low_flag) l_shipmode)
         (keys l_shipmode))))))

;;; ---------------------------------------------------------------------------
;;; Q13 — Customer Distribution
;;; Parameters: customer, orders
;;; Test data: cust=1 has 2 passing orders → c_count=2; cust=2's order has
;;;   "special special requests" → filtered by Not Like → c_count=0; cust=3: no orders.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q13
  (syntax-rules ()
    ((_ customer orders)
     (boss-eval
       (OrderBy
         (GroupBy
           (Project
             (GroupBy
               (LeftJoin
                 customer (keys c_custkey)
                 (Filter orders (Not (Like o_comment "%special%requests%")))
                 (keys o_custkey))
               (Count o_orderkey) c_custkey)
             (As |count(o_orderkey)| c_count))
           (CountAll) c_count)
         (keys (Desc |count_all()|) (Desc c_count)))))))

;;; ---------------------------------------------------------------------------
;;; Q14 — Promotion Effect
;;; Parameters: lineitem, part, shipdate-lo, shipdate-hi
;;; Test data: Sep 1995 = [809913600, 812505600).  partkey=1 "PROMO ANODIZED" →
;;;   promo_revenue=200.0; partkey=2 "STANDARD BOX" → total_revenue+=250.0.  Row 3: date filtered.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q14
  (syntax-rules ()
    ((_ lineitem part shipdate-lo shipdate-hi)
     (boss-eval
       (Project
         (GroupBy
           (Project
             (Join
               (Filter lineitem
                       (And (GreaterEqual l_shipdate shipdate-lo)
                            (Less l_shipdate shipdate-hi)))
               (keys l_partkey)
               part (keys p_partkey))
             (As (IfElse (Like p_type "PROMO%")
                         (Multiply l_extendedprice (Subtract 1.0 l_discount))
                         0.0)
                 promo_disc_price)
             (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) disc_price))
           (Sum promo_disc_price) (Sum disc_price))
         (As |sum(promo_disc_price)| promo_revenue)
         (As |sum(disc_price)| total_revenue))))))

;;; ---------------------------------------------------------------------------
;;; Q16 — Part/Supplier Relationship
;;; Parameters: part, partsupp, supplier
;;; Test data: partkey=1 brand="Brand#12" type="SMALL STEEL" size=3 passes all NOT filters.
;;;   suppkey=1 is clean; suppkey=2 has "Customer Complaints" → excluded by AntiJoin.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q16
  (syntax-rules ()
    ((_ part partsupp supplier)
     (begin
       (boss-eval (Name
         (Filter supplier (Like s_comment "%Customer%Complaints%"))
         q16_complaint_supps))
       (boss-eval
         (OrderBy
           (GroupBy
             (AntiJoin
               (Join
                 (Filter part
                         (And (Not (Equal p_brand "Brand#45"))
                              (And (Not (Like p_type "MEDIUM POLISHED%"))
                                   (Or (Equal p_size 49)
                                       (Or (Equal p_size 14)
                                           (Or (Equal p_size 23)
                                               (Or (Equal p_size 45)
                                                   (Or (Equal p_size 19)
                                                       (Or (Equal p_size 3)
                                                           (Or (Equal p_size 36)
                                                               (Equal p_size 9)))))))))))
                 (keys p_partkey)
                 partsupp (keys ps_partkey))
               (keys ps_suppkey)
               (ByName q16_complaint_supps) (keys s_suppkey))
             (CountAll) p_brand p_type p_size)
           (keys (Desc |count_all()|) p_brand p_type p_size)))))))

;;; ---------------------------------------------------------------------------
;;; Q18 — Large Volume Customer
;;; Parameters: lineitem, customer, orders
;;; Test data: orderkey=1 qty sum=310 > 300 → qualifies; orderkey=2 (qty=100) filtered.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q18
  (syntax-rules ()
    ((_ lineitem customer orders)
     (begin
       (boss-eval (Name
         (Project
           (Filter
             (GroupBy lineitem (Sum l_quantity) l_orderkey)
             (Greater |sum(l_quantity)| 300))
           (As l_orderkey heavy_orderkey))
         q18_heavy))
       (boss-eval
         (Slice
           (OrderBy
             (GroupBy
               (Join
                 (Join
                   (Join customer (keys c_custkey) orders (keys o_custkey))
                   (keys o_orderkey)
                   (ByName q18_heavy) (keys heavy_orderkey))
                 (keys o_orderkey)
                 lineitem (keys l_orderkey))
               (Sum l_quantity) c_name c_custkey o_orderkey o_orderdate o_totalprice)
             (keys (Desc o_totalprice) o_orderdate))
           0 100))))))

;;; ---------------------------------------------------------------------------
;;; Q19 — Discounted Revenue
;;; Parameters: lineitem, part
;;; Test data: Brand#12 (rev=95.0) + Brand#23 (rev=200.0) + Brand#34 (rev=270.0) = 565.0.
;;;   Fourth row filtered (shipinstruct="COLLECT COD").
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q19
  (syntax-rules ()
    ((_ lineitem part)
     (boss-eval
       (GroupBy
         (Project
           (Filter
             (Join lineitem (keys l_partkey) part (keys p_partkey))
             (And (Equal l_shipinstruct "DELIVER IN PERSON")
                  (And (Or (Equal l_shipmode "AIR") (Equal l_shipmode "AIR REG"))
                       (Or
                         (And (Equal p_brand "Brand#12")
                              (And (Or (Equal p_container "SM CASE")
                                       (Or (Equal p_container "SM BOX")
                                           (Or (Equal p_container "SM PACK")
                                               (Equal p_container "SM PKG"))))
                                   (And (GreaterEqual l_quantity 1.0)
                                        (And (LessEqual l_quantity 11.0)
                                             (And (GreaterEqual p_size 1)
                                                  (LessEqual p_size 5))))))
                         (Or
                           (And (Equal p_brand "Brand#23")
                                (And (Or (Equal p_container "MED BAG")
                                         (Or (Equal p_container "MED BOX")
                                             (Or (Equal p_container "MED PKG")
                                                 (Equal p_container "MED PACK"))))
                                     (And (GreaterEqual l_quantity 10.0)
                                          (And (LessEqual l_quantity 20.0)
                                               (And (GreaterEqual p_size 1)
                                                    (LessEqual p_size 10))))))
                           (And (Equal p_brand "Brand#34")
                                (And (Or (Equal p_container "LG CASE")
                                         (Or (Equal p_container "LG BOX")
                                             (Or (Equal p_container "LG PKG")
                                                 (Equal p_container "LG PACK"))))
                                     (And (GreaterEqual l_quantity 20.0)
                                          (And (LessEqual l_quantity 30.0)
                                               (And (GreaterEqual p_size 1)
                                                    (LessEqual p_size 15)))))))))))
           (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) revenue))
         (Sum revenue))))))

;;; ---------------------------------------------------------------------------
;;; Q20 — Potential Part Promotion
;;; Parameters: part, lineitem, partsupp, nation, supplier, shipdate-lo, shipdate-hi
;;; Test data: partkey=1 "forestA" matches "forest%"; partkey=2 filtered.
;;;   1994 lineitem: partkey=1 suppkey=4 qty sum=10.  partsupp: suppkey=4 availqty=6 > 0.5*10=5 ✓;
;;;   suppkey=5 availqty=3 filtered.  suppkey=4 is in CANADA; suppkey=5 in other nation.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q20
  (syntax-rules ()
    ((_ part lineitem partsupp nation supplier shipdate-lo shipdate-hi)
     (begin
       (boss-eval (Name
         (Filter part (Like p_name "forest%"))
         q20_forest_parts))
       (boss-eval (Name
         (GroupBy
           (Filter lineitem
                   (And (GreaterEqual l_shipdate shipdate-lo)
                        (Less l_shipdate shipdate-hi)))
           (Sum l_quantity) l_partkey l_suppkey)
         q20_li_qty))
       (boss-eval (Name
         (Project
           (Filter
             (Join
               (Join
                 partsupp (keys ps_partkey ps_suppkey)
                 (ByName q20_li_qty) (keys l_partkey l_suppkey))
               (keys ps_partkey)
               (ByName q20_forest_parts) (keys p_partkey))
             (Greater ps_availqty (Multiply 0.5 |sum(l_quantity)|)))
           ps_suppkey)
         q20_qualifying_supps))
       (boss-eval
         (OrderBy
           (Project
             (Join
               (Join
                 (Filter nation (Equal n_name "CANADA"))
                 (keys n_nationkey)
                 supplier (keys s_nationkey))
               (keys s_suppkey)
               (ByName q20_qualifying_supps) (keys ps_suppkey))
             s_name s_address)
           (keys s_name)))))))

;;; ---------------------------------------------------------------------------
;;; Q21 — Suppliers Who Kept Orders Waiting
;;; Parameters: lineitem, orders, supplier, nation
;;; Test data: SAUDI ARABIA nation.  orderkey=1: only suppkey=1 is late → kept.
;;;   orderkey=2: suppkey=1+2 both late → multi-late → AntiJoin removes both.
;;;   Result: suppkey=1 (Supp1) contributes 1 qualifying row.
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q21
  (syntax-rules ()
    ((_ lineitem orders supplier nation)
     (begin
       (boss-eval (Name
         (Filter lineitem (Greater l_receiptdate l_commitdate))
         q21_late_li))
       (boss-eval (Name
         (Filter
           (GroupBy (ByName q21_late_li) (CountAll) l_orderkey)
           (Greater |count_all()| 1))
         q21_multi_late_orders))
       (boss-eval
         (Slice
           (OrderBy
             (GroupBy
               (Project
                 (AntiJoin
                   (Join
                     (Join
                       (ByName q21_late_li) (keys l_orderkey)
                       (Filter orders (Equal o_orderstatus "F"))
                       (keys o_orderkey))
                     (keys l_suppkey)
                     (Join supplier (keys s_nationkey)
                           (Filter nation (Equal n_name "SAUDI ARABIA"))
                           (keys n_nationkey))
                     (keys s_suppkey))
                   (keys l_orderkey)
                   (ByName q21_multi_late_orders) (keys l_orderkey))
                 s_name)
               (CountAll) s_name)
             (keys (Desc |count_all()|) s_name))
           0 100))))))
