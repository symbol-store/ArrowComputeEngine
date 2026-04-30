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

(define (date-string->day-count s)
  ;; "YYYY-MM-DD" → integer days since unix epoch (matches Arrow date32[day]).
  ;; make-tm args: sec min hour day month-0based year-since-1900 isdst
  (let ((year  (string->number (substring s 0 4)))
        (month (string->number (substring s 5 7)))
        (day   (string->number (substring s 8 10))))
    (exact (floor (/ (time->seconds (make-tm 0 0 0 day (- month 1) (- year 1900) 0))
                     86400)))))

(define (date->days s)
  ;; Wraps the day count in a BOSS Date() expression so Arrow casts it to date32.
  (list 'Date (date-string->day-count s)))

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
               (Project
                 (Join
                   (Join
                     (Filter customer (Equal c_mktsegment "BUILDING"))
                     (keys c_custkey)
                     (Filter orders (Less o_orderdate cutoff))
                     (keys o_custkey))
                   (keys o_orderkey)
                   (Filter lineitem (Greater l_shipdate cutoff))
                   (keys l_orderkey))
                 l_orderkey o_orderdate o_shippriority
                 (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) disc_price))
               (Sum disc_price) l_orderkey o_orderdate o_shippriority)
             l_orderkey
             (As |sum(disc_price)| revenue)
             o_orderdate o_shippriority)
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
               (Project
                 (Join
                   lineitem (keys l_orderkey l_suppkey)
                   (ByName q5_ord_supp) (keys o_orderkey s_suppkey))
                 n_name
                 (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) disc_price))
               (Sum disc_price) n_name)
             n_name (As |sum(disc_price)| revenue))
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
         (Project
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
             (keys o_year))
           o_year
           (As (Divide |sum(brazil_volume)| |sum(volume)|) mkt_share)))))))


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
               (Project
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
                 c_custkey c_name c_acctbal c_address c_phone c_comment n_name
                 (As (Multiply l_extendedprice (Subtract 1.0 l_discount)) disc_price))
               (Sum disc_price) c_custkey c_name c_acctbal c_address c_phone c_comment n_name)
             c_custkey c_name
             (As |sum(disc_price)| revenue)
             c_acctbal n_name c_address c_phone c_comment)
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
         (As (Multiply 100.0 (Divide |sum(promo_disc_price)| |sum(disc_price)|))
             promo_revenue))))))

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
;;; Test data: SAUDI ARABIA nation.
;;;   orderkey=1: suppkey=1 (late) + suppkey=3 (on-time, OTHER) → Supp1 qualifies.
;;;   orderkey=2: suppkey=1 (late) + suppkey=2 (late) → multi-late → AntiJoin removes both.
;;;   Result: suppkey=1 (Supp1) contributes 1 qualifying row.
;;; Conditions implemented:
;;;   (1) l_receiptdate > l_commitdate (late)
;;;   (2) order has another supplier — enforced via join with q21_multi_supp_orders
;;;   (3) no other LATE supplier — enforced via AntiJoin with q21_multi_late_orders
;;;       Both (2) and (3) use distinct-supplier counts (not raw lineitem counts).
;;; ---------------------------------------------------------------------------
(define-syntax tpch-q21
  (syntax-rules ()
    ((_ lineitem orders supplier nation)
     (begin
       ;; Condition (1): late lineitems
       (boss-eval (Name
         (Filter lineitem (Greater l_receiptdate l_commitdate))
         q21_late_li))
       ;; Condition (2): orders with at least 2 distinct suppliers.
       ;; min(suppkey) ≠ max(suppkey) iff there are 2+ distinct suppkeys.
       (boss-eval (Name
         (Filter
           (GroupBy lineitem (Min l_suppkey) (Max l_suppkey) l_orderkey)
           (Not (Equal |min(l_suppkey)| |max(l_suppkey)|)))
         q21_multi_supp_orders))
       ;; Condition (3): orders with at least 2 distinct LATE suppliers (exclusion list).
       (boss-eval (Name
         (Filter
           (GroupBy (ByName q21_late_li) (Min l_suppkey) (Max l_suppkey) l_orderkey)
           (Not (Equal |min(l_suppkey)| |max(l_suppkey)|)))
         q21_multi_late_orders))
       (boss-eval
         (Slice
           (OrderBy
             (GroupBy
               (Project
                 (AntiJoin
                   (Join
                     (Join
                       ;; Condition (2) semi-join: Project renames l_orderkey_l back to l_orderkey.
                       (Project
                         (Join (ByName q21_late_li) (keys l_orderkey)
                               (ByName q21_multi_supp_orders) (keys l_orderkey))
                         (As l_orderkey_l l_orderkey) l_suppkey)
                       (keys l_orderkey)
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

;;; ---------------------------------------------------------------------------
;;; Result-checking utilities
;;;
;;; check-expected compares a BOSS Table result against a list of expected rows.
;;; Numeric values use a 0.1% relative tolerance to accommodate float differences
;;; between database engines.  Date columns come back from Arrow as day counts
;;; (int32), matching the integer values in the tpch-q*-expected lists below.
;;; ---------------------------------------------------------------------------

;; Transpose (Table (col v ...) ...) into a list of rows ((v v ...) ...).
(define (table->rows tbl)
  (let* ((cols  (map cdr (cdr tbl)))
         (nrows (length (car cols))))
    (let loop ((i 0) (rows '()))
      (if (= i nrows)
          (reverse rows)
          (loop (+ i 1)
                (cons (map (lambda (col) (list-ref col i)) cols)
                      rows))))))

;; Value equality: 0.1% relative tolerance for any inexact number, exact otherwise.
(define (result-value=? a b)
  (if (and (number? a) (number? b) (or (inexact? a) (inexact? b)))
      (let ((scale (max (abs (inexact a)) (abs (inexact b)) 1.0)))
        (< (abs (- (inexact a) (inexact b))) (* 1e-3 scale)))
      (equal? a b)))

;; Return #t if the BOSS Table result matches the given expected rows.
(define (check-expected result expected)
  (let ((actual (table->rows result)))
    (and (= (length actual) (length expected))
         (let loop ((a actual) (e expected))
           (or (null? a)
               (and (let row-loop ((ra (car a)) (re (car e)))
                      (cond ((and (null? ra) (null? re)) #t)
                            ((or  (null? ra) (null? re)) #f)
                            ((result-value=? (car ra) (car re))
                             (row-loop (cdr ra) (cdr re)))
                            (else #f)))
                    (loop (cdr a) (cdr e))))))))

;;; Expected results extracted from MonetDB .out reference files (SF10).
;;; Dates are stored as day counts (matching Arrow date32[day] output).

(define tpch-q1-expected
  '(("A" "F" 380456.0 532348211.65 505822441.4861 526165934.000839 25.57515461 35785.70931 0.05008133907 14876)
    ("N" "F" 8971.0 12384801.37 11798257.208 12282485.056933 25.77873563 35588.50968 0.04775862069 348)
    ("N" "O" 742802.0 1041502841.45 989737518.6346 1029418531.52335 25.45498783 35691.12921 0.04993111956 29181)
    ("R" "F" 381449.0 534594445.35 507996454.4067 528524219.358903 25.59716817 35874.00653 0.04982753993 14902)))

(define tpch-q2-expected
  '((4186.95 "Supplier#000000077" "GERMANY" 249 "Manufacturer#4" "wVtcr0uH3CyrSiWMLsqnB09Syo,UuZxPMeBghlY" "17-281-345-4863" "blithely pending accounts ru")
    (1883.37 "Supplier#000000086" "ROMANIA" 1015 "Manufacturer#4" "J1fgg5QaqnN" "29-903-665-7065" "slyly pending requests wake car")
    (1687.81 "Supplier#000000017" "ROMANIA" 1634 "Manufacturer#2" "c2d,ESHRSkK3WYnxpgw6aOqN0q" "29-601-884-9219" "slyly pending deposits boost quickly.")
    (287.16 "Supplier#000000052" "ROMANIA" 323 "Manufacturer#4" "WCk XCHYzBA1dvJDSol4ZJQQcQN," "29-974-934-4713" "ironic accounts are slyly above the foxes. regular courts use furiously! slyly pending deposi")))

(define tpch-q3-expected
  '((47714 267010.5894 9200 0)
    (22276 266351.5562 9159 0)
    (32965 263768.3414 9186 0)
    (21956 254541.1285 9163 0)
    (1637  243512.7981 9169 0)
    (10916 241320.0814 9200 0)
    (30497 208566.6969 9168 0)
    (450   205447.4232 9194 0)
    (47204 204478.5213 9202 0)
    (9696  201502.2188 9181 0)))

(define tpch-q4-expected
  '(("1-URGENT"        93)
    ("2-HIGH"         103)
    ("3-MEDIUM"       109)
    ("4-NOT SPECIFIED" 102)
    ("5-LOW"          128)))

(define tpch-q5-expected
  '(("VIETNAM"   1000926.6999)
    ("CHINA"      740210.757)
    ("JAPAN"      660651.2425)
    ("INDONESIA"  566379.5276)
    ("INDIA"      422874.6844)))

(define tpch-q6-expected
  '((1193053.2253)))

(define tpch-q7-expected
  '(("FRANCE"  "GERMANY" 1995 268068.5774)
    ("FRANCE"  "GERMANY" 1996 303862.298)
    ("GERMANY" "FRANCE"  1995 621159.4882)
    ("GERMANY" "FRANCE"  1996 379095.8854)))

(define tpch-q8-expected
  '((1995 0.0)
    (1996 0.0)))

(define tpch-q9-expected
  '(("ALGERIA"        1998 386617.8283)  ("ALGERIA"        1997 401601.1258)
    ("ALGERIA"        1996 156938.7971)  ("ALGERIA"        1995 486706.0631)
    ("ALGERIA"        1994 426573.4415)  ("ALGERIA"        1993 448371.4336)
    ("ALGERIA"        1992 285188.4503)  ("ARGENTINA"      1998 136458.3471)
    ("ARGENTINA"      1997 423877.6754)  ("ARGENTINA"      1996 325019.6263)
    ("ARGENTINA"      1995 399583.4009)  ("ARGENTINA"      1994 374046.7888)
    ("ARGENTINA"      1993 351669.5325)  ("ARGENTINA"      1992 312412.4307)
    ("BRAZIL"         1998 265032.7654)  ("BRAZIL"         1997 275700.7887)
    ("BRAZIL"         1996 490362.6267)  ("BRAZIL"         1995 349754.1082)
    ("BRAZIL"         1994 348798.4066)  ("BRAZIL"         1993 457273.7693)
    ("BRAZIL"         1992 280563.4508)  ("CANADA"         1998 249632.9034)
    ("CANADA"         1997 486581.6822)  ("CANADA"         1996 359393.5226)
    ("CANADA"         1995 243051.8308)  ("CANADA"         1994 188897.4377)
    ("CANADA"         1993 546007.0343)  ("CANADA"         1992 275013.265)
    ("CHINA"          1998 391223.3132)  ("CHINA"          1997 585354.2015)
    ("CHINA"          1996 555841.012)   ("CHINA"          1995 864856.4126)
    ("CHINA"          1994 669658.4851)  ("CHINA"          1993 621317.0197)
    ("CHINA"          1992 932500.7559)  ("EGYPT"          1998 367340.0376)
    ("EGYPT"          1997 958524.1565)  ("EGYPT"          1996 417700.6911)
    ("EGYPT"          1995 852185.4611)  ("EGYPT"          1994 442097.3675)
    ("EGYPT"          1993 677948.0185)  ("EGYPT"          1992 666425.4212)
    ("ETHIOPIA"       1998 146634.8925)  ("ETHIOPIA"       1997 264122.6167)
    ("ETHIOPIA"       1996 193275.0975)  ("ETHIOPIA"       1995 220253.5131)
    ("ETHIOPIA"       1994 296634.26)    ("ETHIOPIA"       1993 304224.8129)
    ("ETHIOPIA"       1992 297588.3116)  ("FRANCE"         1998  72506.5)
    ("FRANCE"         1997 237462.124)   ("FRANCE"         1996 151017.7675)
    ("FRANCE"         1995 296667.9453)  ("FRANCE"         1994 233805.7419)
    ("FRANCE"         1993 168968.155)   ("FRANCE"         1992 127349.1738)
    ("GERMANY"        1998 223811.2759)  ("GERMANY"        1997 661263.7764)
    ("GERMANY"        1996 482126.6721)  ("GERMANY"        1995 571466.4843)
    ("GERMANY"        1994 322330.4404)  ("GERMANY"        1993 428314.7853)
    ("GERMANY"        1992 273675.9499)  ("INDIA"          1998 418144.1956)
    ("INDIA"          1997 859947.3428)  ("INDIA"          1996 515838.8397)
    ("INDIA"          1995 631351.5802)  ("INDIA"          1994 798279.5615)
    ("INDIA"          1993 767946.7017)  ("INDIA"          1992 797101.9729)
    ("INDONESIA"      1998 386787.9168)  ("INDONESIA"      1997 311837.4839)
    ("INDONESIA"      1996 421631.7918)  ("INDONESIA"      1995 479331.3577)
    ("INDONESIA"      1994 602376.904)   ("INDONESIA"      1993 496450.6942)
    ("INDONESIA"      1992 561262.1781)  ("IRAN"           1998   8996.554)
    ("IRAN"           1997 201653.8389)  ("IRAN"           1996 281658.4382)
    ("IRAN"           1995  50873.1323)  ("IRAN"           1994  53387.1992)
    ("IRAN"           1993 107749.9627)  ("IRAN"           1992  67888.7176)
    ("IRAQ"           1998 113434.1032)  ("IRAQ"           1997  86656.8062)
    ("IRAQ"           1996 359937.8761)  ("IRAQ"           1995 218221.7756)
    ("IRAQ"           1994 360489.8843)  ("IRAQ"           1993 559990.6546)
    ("IRAQ"           1992 211655.9396)  ("JAPAN"          1998 278531.8011)
    ("JAPAN"          1997 426945.7933)  ("JAPAN"          1996 501942.5698)
    ("JAPAN"          1995 474025.8492)  ("JAPAN"          1994 706404.4339)
    ("JAPAN"          1993 695412.9084)  ("JAPAN"          1992 613125.5417)
    ("JORDAN"         1998  73080.7362)  ("JORDAN"         1997 117104.2978)
    ("JORDAN"         1996  94740.7164)  ("JORDAN"         1995 164684.4569)
    ("JORDAN"         1994  51403.2065)  ("JORDAN"         1993  38718.7839)
    ("JORDAN"         1992 132028.5385)  ("KENYA"          1998 351661.8184)
    ("KENYA"          1997 542347.9571)  ("KENYA"          1996 466964.0397)
    ("KENYA"          1995 795396.7551)  ("KENYA"          1994 740881.7388)
    ("KENYA"          1993 603341.1861)  ("KENYA"          1992 774761.2393)
    ("MOROCCO"        1998 118171.3902)  ("MOROCCO"        1997  96442.7008)
    ("MOROCCO"        1996 118984.8785)  ("MOROCCO"        1995 158240.6598)
    ("MOROCCO"        1994 148951.6794)  ("MOROCCO"        1993  48279.6548)
    ("MOROCCO"        1992 146068.255)   ("MOZAMBIQUE"     1998 343227.8816)
    ("MOZAMBIQUE"     1997 831834.1044)  ("MOZAMBIQUE"     1996 888199.0121)
    ("MOZAMBIQUE"     1995 1249272.9387) ("MOZAMBIQUE"     1994 594096.0637)
    ("MOZAMBIQUE"     1993 1200185.0713) ("MOZAMBIQUE"     1992 994120.0362)
    ("PERU"           1998 352324.2789)  ("PERU"           1997 319502.2255)
    ("PERU"           1996 391644.9686)  ("PERU"           1995 360028.0705)
    ("PERU"           1994 460058.1291)  ("PERU"           1993 382460.0831)
    ("PERU"           1992 312613.1714)  ("ROMANIA"        1998 340984.6297)
    ("ROMANIA"        1997 444095.1884)  ("ROMANIA"        1996 426472.5967)
    ("ROMANIA"        1995 616350.9394)  ("ROMANIA"        1994 430563.1943)
    ("ROMANIA"        1993 769406.9533)  ("ROMANIA"        1992 543722.1295)
    ("RUSSIA"         1998 217747.8262)  ("RUSSIA"         1997 644719.5017)
    ("RUSSIA"         1996 501019.7684)  ("RUSSIA"         1995 717528.7447)
    ("RUSSIA"         1994 441262.635)   ("RUSSIA"         1993 529422.5932)
    ("RUSSIA"         1992 469683.7369)  ("SAUDI ARABIA"   1998  57980.2356)
    ("SAUDI ARABIA"   1997  17173.121)   ("SAUDI ARABIA"   1996  14229.6253)
    ("SAUDI ARABIA"   1995  98053.2309)  ("SAUDI ARABIA"   1993  42289.631)
    ("SAUDI ARABIA"   1992  50978.9572)  ("UNITED KINGDOM" 1998 127808.1215)
    ("UNITED KINGDOM" 1997 407935.6606)  ("UNITED KINGDOM" 1996 499957.5199)
    ("UNITED KINGDOM" 1995 480575.5026)  ("UNITED KINGDOM" 1994 513252.8116)
    ("UNITED KINGDOM" 1993 697570.9412)  ("UNITED KINGDOM" 1992 361516.4116)
    ("UNITED STATES"  1998 503864.6963)  ("UNITED STATES"  1997 649175.2847)
    ("UNITED STATES"  1996 831723.1557)  ("UNITED STATES"  1995 902131.2862)
    ("UNITED STATES"  1994 460768.5468)  ("UNITED STATES"  1993 656092.8661)
    ("UNITED STATES"  1992 714228.6231)  ("VIETNAM"        1998 578857.041)
    ("VIETNAM"        1997 596114.8585)  ("VIETNAM"        1996 832979.053)
    ("VIETNAM"        1995 757862.0438)  ("VIETNAM"        1994 1003275.5371)
    ("VIETNAM"        1993 461389.0037)  ("VIETNAM"        1992 820665.7064)))

(define tpch-q10-expected
  '((679  "Customer#000000679" 378211.3252   1394.44 "IRAN"           "IJf1FlZL9I9m,rvofcoKy5pRUOjUQV"          "20-146-696-9508" "regular ideas promise against the furiously final deposits. f")
    (1201 "Customer#000001201" 374331.534    5165.39 "IRAN"           "LfCSVKWozyWOGDW02g9UX,XgH5YU2o5ql1zBrN"  "20-825-400-1187" "fluffily final grouches doubt. bold dependencies dazzle caref")
    (422  "Customer#000000422" 366451.0126   -272.14 "INDONESIA"      "AyNzZBvmIDo42JtjP9xzaK3pnvkh Qc0o08ssnvq" "19-299-247-2444" "furiously ironic asymptotes are slyly ironic, ironic requests. bold,")
    (334  "Customer#000000334" 360370.755    -405.91 "EGYPT"          "OPN1N7t4aQ23TnCpc"                        "14-947-291-5002" "packages boost blithely carefully final depend")
    (805  "Customer#000000805" 359448.9036    511.69 "IRAN"           "wCKx5zcHvwpSffyc9qfi9dvqcm9LT,cLAG"       "20-732-989-5653" "slyly ironic ideas about the regular packages cajole ironic deposits. furiously regular notornis affix b")
    (932  "Customer#000000932" 341608.2753   6553.37 "JORDAN"         "HN9Ap0NsJG7Mb8O"                          "23-300-708-7927" "blithely ironic frets above the quickly even courts hagg")
    (853  "Customer#000000853" 341236.6246   -444.73 "BRAZIL"         "U0 9PrwAgWK8AE0GHmnCGtH9BTexWWv87k"       "12-869-161-3468" "special, daring packages above the care")
    (872  "Customer#000000872" 338328.7808   -858.61 "PERU"           "vLP7iNZBK4B,HANFTKabVI3AO Y9O8H"          "27-357-139-7164" "dogged accounts sleep carefully pending ep")
    (737  "Customer#000000737" 338185.3365   2501.74 "CHINA"          "NdjG1k243iCLSoy1lYqMIrpvuH1Uf75"          "28-658-938-1102" "bold theodolites are furiously. blithely express p")
    (1118 "Customer#000001118" 319875.728    4130.18 "IRAQ"           "QHg,DNvEVXaYoCdrywazjAJ"                  "21-583-715-8627" "furiously final pinto beans maintain. ")
    (223  "Customer#000000223" 319564.275    7476.2  "SAUDI ARABIA"   "ftau6Pk,brboMyEl,,kFm"                    "30-193-643-1517" "carefully regular deposits boost after the even foxes. slyly even requests haggle. ")
    (808  "Customer#000000808" 314774.6167   5561.93 "ROMANIA"        "S2WkSKCGtnbhcFOp6MWcuB3rzFlFemVNrg "      "29-531-319-7726" "ironic, special gifts according to the quickly final deposits impress blith")
    (478  "Customer#000000478" 299651.8026   -210.4  "ARGENTINA"      "clyq458DIkXXt4qLyHlbe,n JueoniF"          "11-655-291-2694" "even notornis nag slyly ironic instructions. fluf")
    (1441 "Customer#000001441" 294705.3935   9465.15 "UNITED KINGDOM" "u0YYZb46w,pwKo5H9vz d6B9zK4BOHhG jx"     "33-681-334-4499" "even theodolites nag. slyly silent requests use slyly among the ironic theodolites. blithely ironic")
    (1478 "Customer#000001478" 294431.9178   9701.54 "GERMANY"        "x7HDvJDDpR3MqZ5vg2CanfQ1hF0j4"            "17-420-484-5959" "express, ironic requests haggle carefully according to th")
    (211  "Customer#000000211" 287905.6368   4198.72 "JORDAN"         "URhlVPzz4FqXem"                            "23-965-335-9471" "instructions lose carefully against the carefully expres")
    (197  "Customer#000000197" 283190.4807   9860.22 "ARGENTINA"      "UeVqssepNuXmtZ38D"                        "11-107-312-6585" "regular, ironic packages about the s")
    (1030 "Customer#000001030" 282557.3566   6359.27 "INDIA"          "Xpt1BiB5h9o"                               "18-759-877-1870" "carefully express asymptotes are after the blithe deposits. regular pl")
    (1049 "Customer#000001049" 281134.1117   8747.99 "INDONESIA"      "bZ1OcFhHaIZ5gMiH"                         "19-499-258-2851" "ironic, unusual dependencies sleep across the quickly reg")
    (1094 "Customer#000001094" 274877.444    2544.49 "BRAZIL"         "OFz0eedTmPmXk2 3XM9v9Mcp13NVC0PK"         "12-234-721-9871" "regular, regular accounts boost special instructions. bold, final accounts at the regular pa")))

(define tpch-q11-expected
  '((1376 13271249.89)))

(define tpch-q12-expected
  '(("MAIL" 64 86)
    ("SHIP" 61 96)))

(define tpch-q13-expected
  '((0 500) (11 70) (10 70) (9 62) (12 61) (8 61) (13 55) (14 52) (20 48)
    (19 46) (16 44) (7 44) (21 42) (15 42) (18 40) (17 38) (22 35) (6 32)
    (24 31) (23 30) (25 20) (26 19) (5 15) (27 12) (29 8) (4 8) (28 4)
    (32 3) (31 3) (3 2) (2 2) (30 1)))

(define tpch-q14-expected
  '((15.48654)))

(define tpch-q16-expected
  '(("Brand#14" "PROMO BRUSHED STEEL"      9 8)
    ("Brand#22" "LARGE BURNISHED TIN"     36 8)
    ("Brand#35" "SMALL POLISHED COPPER"   14 8)
    ("Brand#11" "ECONOMY BURNISHED NICKEL" 49 4) ("Brand#11" "LARGE PLATED TIN"         23 4)
    ("Brand#11" "MEDIUM ANODIZED BRASS"   45 4) ("Brand#11" "MEDIUM BRUSHED BRASS"    45 4)
    ("Brand#11" "PROMO ANODIZED BRASS"     3 4) ("Brand#11" "PROMO ANODIZED BRASS"    49 4)
    ("Brand#11" "PROMO ANODIZED TIN"      45 4) ("Brand#11" "PROMO BURNISHED BRASS"   36 4)
    ("Brand#11" "SMALL ANODIZED TIN"      45 4) ("Brand#11" "SMALL PLATED COPPER"     45 4)
    ("Brand#11" "STANDARD POLISHED NICKEL" 45 4) ("Brand#11" "STANDARD POLISHED TIN"  45 4)
    ("Brand#12" "ECONOMY BURNISHED COPPER" 45 4) ("Brand#12" "LARGE ANODIZED TIN"     45 4)
    ("Brand#12" "LARGE BURNISHED BRASS"   19 4) ("Brand#12" "LARGE PLATED STEEL"      36 4)
    ("Brand#12" "MEDIUM PLATED BRASS"     23 4) ("Brand#12" "PROMO BRUSHED COPPER"    14 4)
    ("Brand#12" "PROMO BURNISHED BRASS"   49 4) ("Brand#12" "SMALL ANODIZED COPPER"   23 4)
    ("Brand#12" "STANDARD ANODIZED BRASS"  3 4) ("Brand#12" "STANDARD BURNISHED TIN"  23 4)
    ("Brand#12" "STANDARD PLATED STEEL"   36 4) ("Brand#13" "ECONOMY PLATED STEEL"    23 4)
    ("Brand#13" "ECONOMY POLISHED BRASS"   9 4) ("Brand#13" "ECONOMY POLISHED COPPER"  9 4)
    ("Brand#13" "LARGE ANODIZED TIN"      19 4) ("Brand#13" "LARGE BURNISHED TIN"     49 4)
    ("Brand#13" "LARGE POLISHED BRASS"     3 4) ("Brand#13" "MEDIUM ANODIZED STEEL"   36 4)
    ("Brand#13" "MEDIUM PLATED COPPER"    19 4) ("Brand#13" "PROMO BRUSHED COPPER"    49 4)
    ("Brand#13" "PROMO PLATED TIN"        19 4) ("Brand#13" "SMALL BRUSHED NICKEL"    19 4)
    ("Brand#13" "SMALL BURNISHED BRASS"   45 4) ("Brand#14" "ECONOMY ANODIZED STEEL"  19 4)
    ("Brand#14" "ECONOMY BURNISHED TIN"   23 4) ("Brand#14" "ECONOMY PLATED STEEL"    45 4)
    ("Brand#14" "ECONOMY PLATED TIN"       9 4) ("Brand#14" "LARGE ANODIZED NICKEL"    9 4)
    ("Brand#14" "LARGE BRUSHED NICKEL"    45 4) ("Brand#14" "SMALL ANODIZED NICKEL"   45 4)
    ("Brand#14" "SMALL BURNISHED COPPER"  14 4) ("Brand#14" "SMALL BURNISHED TIN"     23 4)
    ("Brand#15" "ECONOMY ANODIZED STEEL"  36 4) ("Brand#15" "ECONOMY BRUSHED BRASS"   36 4)
    ("Brand#15" "ECONOMY BURNISHED BRASS" 14 4) ("Brand#15" "ECONOMY PLATED STEEL"    45 4)
    ("Brand#15" "LARGE ANODIZED BRASS"    45 4) ("Brand#15" "LARGE ANODIZED COPPER"    3 4)
    ("Brand#15" "MEDIUM ANODIZED COPPER"   9 4) ("Brand#15" "MEDIUM PLATED TIN"        9 4)
    ("Brand#15" "PROMO POLISHED TIN"      49 4) ("Brand#15" "SMALL POLISHED STEEL"    19 4)
    ("Brand#15" "STANDARD BURNISHED STEEL" 45 4) ("Brand#15" "STANDARD PLATED NICKEL" 19 4)
    ("Brand#15" "STANDARD PLATED TIN"      3 4) ("Brand#21" "ECONOMY ANODIZED STEEL"  19 4)
    ("Brand#21" "ECONOMY BRUSHED TIN"     49 4) ("Brand#21" "LARGE BURNISHED COPPER"  19 4)
    ("Brand#21" "MEDIUM ANODIZED TIN"      9 4) ("Brand#21" "MEDIUM BURNISHED STEEL"  23 4)
    ("Brand#21" "PROMO BRUSHED STEEL"     23 4) ("Brand#21" "PROMO BURNISHED COPPER"  19 4)
    ("Brand#21" "STANDARD PLATED BRASS"   49 4) ("Brand#21" "STANDARD POLISHED TIN"   36 4)
    ("Brand#22" "ECONOMY BURNISHED NICKEL" 19 4) ("Brand#22" "LARGE ANODIZED STEEL"    3 4)
    ("Brand#22" "LARGE BURNISHED STEEL"   23 4) ("Brand#22" "LARGE BURNISHED STEEL"   45 4)
    ("Brand#22" "LARGE BURNISHED TIN"     45 4) ("Brand#22" "LARGE POLISHED NICKEL"   19 4)
    ("Brand#22" "MEDIUM ANODIZED TIN"      9 4) ("Brand#22" "MEDIUM BRUSHED BRASS"    14 4)
    ("Brand#22" "MEDIUM BRUSHED COPPER"    3 4) ("Brand#22" "MEDIUM BRUSHED COPPER"   45 4)
    ("Brand#22" "MEDIUM BURNISHED TIN"    19 4) ("Brand#22" "MEDIUM BURNISHED TIN"    23 4)
    ("Brand#22" "MEDIUM PLATED BRASS"     49 4) ("Brand#22" "PROMO BRUSHED BRASS"      9 4)
    ("Brand#22" "PROMO BRUSHED STEEL"     36 4) ("Brand#22" "SMALL BRUSHED NICKEL"     3 4)
    ("Brand#22" "SMALL BURNISHED STEEL"   23 4) ("Brand#22" "STANDARD PLATED NICKEL"   3 4)
    ("Brand#22" "STANDARD PLATED TIN"     19 4) ("Brand#23" "ECONOMY BRUSHED COPPER"   9 4)
    ("Brand#23" "LARGE ANODIZED COPPER"   14 4) ("Brand#23" "LARGE PLATED BRASS"       49 4)
    ("Brand#23" "MEDIUM BRUSHED NICKEL"    3 4) ("Brand#23" "PROMO ANODIZED COPPER"   19 4)
    ("Brand#23" "PROMO BURNISHED COPPER"  14 4) ("Brand#23" "PROMO POLISHED BRASS"    14 4)
    ("Brand#23" "SMALL BRUSHED BRASS"     49 4) ("Brand#23" "SMALL BRUSHED COPPER"    45 4)
    ("Brand#23" "SMALL BURNISHED COPPER"  49 4) ("Brand#23" "SMALL PLATED BRASS"      36 4)
    ("Brand#23" "SMALL POLISHED BRASS"     9 4) ("Brand#23" "STANDARD BRUSHED TIN"     3 4)
    ("Brand#23" "STANDARD PLATED BRASS"    9 4) ("Brand#23" "STANDARD PLATED STEEL"   36 4)
    ("Brand#23" "STANDARD PLATED TIN"     19 4) ("Brand#24" "ECONOMY BRUSHED BRASS"   36 4)
    ("Brand#24" "ECONOMY PLATED COPPER"   36 4) ("Brand#24" "LARGE PLATED NICKEL"     36 4)
    ("Brand#24" "MEDIUM PLATED STEEL"     19 4) ("Brand#24" "PROMO POLISHED BRASS"    14 4)
    ("Brand#24" "SMALL ANODIZED COPPER"    3 4) ("Brand#24" "STANDARD BRUSHED BRASS"  14 4)
    ("Brand#24" "STANDARD BRUSHED STEEL"  14 4) ("Brand#24" "STANDARD POLISHED NICKEL" 14 4)
    ("Brand#25" "ECONOMY BURNISHED TIN"   19 4) ("Brand#25" "ECONOMY PLATED NICKEL"   23 4)
    ("Brand#25" "LARGE ANODIZED NICKEL"   23 4) ("Brand#25" "LARGE BRUSHED NICKEL"    19 4)
    ("Brand#25" "LARGE BURNISHED TIN"     49 4) ("Brand#25" "MEDIUM BURNISHED NICKEL" 49 4)
    ("Brand#25" "MEDIUM PLATED BRASS"     45 4) ("Brand#25" "PROMO ANODIZED TIN"       3 4)
    ("Brand#25" "PROMO BURNISHED COPPER"  45 4) ("Brand#25" "PROMO PLATED NICKEL"      3 4)
    ("Brand#25" "SMALL BURNISHED COPPER"   3 4) ("Brand#25" "SMALL PLATED TIN"        36 4)
    ("Brand#25" "STANDARD ANODIZED TIN"    9 4) ("Brand#25" "STANDARD PLATED NICKEL"  36 4)
    ("Brand#31" "ECONOMY BURNISHED COPPER" 36 4) ("Brand#31" "ECONOMY PLATED STEEL"   23 4)
    ("Brand#31" "LARGE PLATED NICKEL"     14 4) ("Brand#31" "MEDIUM BURNISHED COPPER"  3 4)
    ("Brand#31" "MEDIUM PLATED TIN"       36 4) ("Brand#31" "PROMO ANODIZED NICKEL"    9 4)
    ("Brand#31" "PROMO POLISHED TIN"      23 4) ("Brand#31" "SMALL ANODIZED COPPER"    3 4)
    ("Brand#31" "SMALL ANODIZED COPPER"   45 4) ("Brand#31" "SMALL BRUSHED NICKEL"    23 4)
    ("Brand#31" "SMALL PLATED COPPER"     36 4) ("Brand#32" "ECONOMY ANODIZED COPPER" 36 4)
    ("Brand#32" "ECONOMY PLATED COPPER"    9 4) ("Brand#32" "LARGE ANODIZED STEEL"    14 4)
    ("Brand#32" "MEDIUM ANODIZED STEEL"   49 4) ("Brand#32" "MEDIUM BURNISHED BRASS"   9 4)
    ("Brand#32" "MEDIUM BURNISHED BRASS"  49 4) ("Brand#32" "PROMO BRUSHED STEEL"     23 4)
    ("Brand#32" "PROMO BURNISHED TIN"     45 4) ("Brand#32" "SMALL ANODIZED TIN"       9 4)
    ("Brand#32" "SMALL BRUSHED COPPER"     3 4) ("Brand#32" "SMALL PLATED COPPER"     45 4)
    ("Brand#32" "SMALL POLISHED STEEL"    36 4) ("Brand#32" "SMALL POLISHED TIN"      45 4)
    ("Brand#32" "STANDARD PLATED STEEL"   36 4) ("Brand#33" "ECONOMY BURNISHED COPPER" 14 4)
    ("Brand#33" "ECONOMY POLISHED BRASS"  14 4) ("Brand#33" "LARGE BRUSHED TIN"       36 4)
    ("Brand#33" "MEDIUM ANODIZED BRASS"    3 4) ("Brand#33" "MEDIUM BURNISHED COPPER" 14 4)
    ("Brand#33" "MEDIUM PLATED STEEL"     49 4) ("Brand#33" "PROMO PLATED STEEL"      49 4)
    ("Brand#33" "PROMO PLATED TIN"        49 4) ("Brand#33" "PROMO POLISHED STEEL"     9 4)
    ("Brand#33" "SMALL ANODIZED COPPER"   23 4) ("Brand#33" "SMALL BRUSHED STEEL"      3 4)
    ("Brand#33" "SMALL BURNISHED NICKEL"   3 4) ("Brand#33" "STANDARD PLATED NICKEL"  36 4)
    ("Brand#34" "ECONOMY ANODIZED TIN"    49 4) ("Brand#34" "LARGE ANODIZED BRASS"    23 4)
    ("Brand#34" "LARGE BRUSHED COPPER"    23 4) ("Brand#34" "LARGE BURNISHED TIN"     49 4)
    ("Brand#34" "LARGE PLATED BRASS"      45 4) ("Brand#34" "MEDIUM BRUSHED COPPER"    9 4)
    ("Brand#34" "MEDIUM BRUSHED TIN"      14 4) ("Brand#34" "MEDIUM BURNISHED NICKEL"  3 4)
    ("Brand#34" "SMALL ANODIZED STEEL"    23 4) ("Brand#34" "SMALL BRUSHED TIN"        9 4)
    ("Brand#34" "SMALL PLATED BRASS"      14 4) ("Brand#34" "STANDARD ANODIZED NICKEL" 36 4)
    ("Brand#34" "STANDARD BRUSHED TIN"    19 4) ("Brand#34" "STANDARD BURNISHED TIN"  23 4)
    ("Brand#34" "STANDARD PLATED NICKEL"  36 4) ("Brand#35" "PROMO BURNISHED BRASS"    3 4)
    ("Brand#35" "PROMO BURNISHED STEEL"   14 4) ("Brand#35" "PROMO PLATED BRASS"      19 4)
    ("Brand#35" "STANDARD ANODIZED NICKEL" 14 4) ("Brand#35" "STANDARD ANODIZED STEEL" 23 4)
    ("Brand#35" "STANDARD BRUSHED BRASS"   3 4) ("Brand#35" "STANDARD BRUSHED NICKEL" 49 4)
    ("Brand#35" "STANDARD PLATED STEEL"   14 4) ("Brand#41" "MEDIUM ANODIZED NICKEL"   9 4)
    ("Brand#41" "MEDIUM BRUSHED TIN"       9 4) ("Brand#41" "MEDIUM PLATED STEEL"     19 4)
    ("Brand#41" "PROMO ANODIZED NICKEL"    9 4) ("Brand#41" "SMALL ANODIZED STEEL"    45 4)
    ("Brand#41" "SMALL POLISHED COPPER"   14 4) ("Brand#41" "STANDARD ANODIZED NICKEL" 9 4)
    ("Brand#41" "STANDARD ANODIZED TIN"   36 4) ("Brand#41" "STANDARD ANODIZED TIN"   49 4)
    ("Brand#41" "STANDARD BRUSHED TIN"    45 4) ("Brand#41" "STANDARD PLATED TIN"     49 4)
    ("Brand#42" "ECONOMY BRUSHED COPPER"  14 4) ("Brand#42" "LARGE ANODIZED NICKEL"   49 4)
    ("Brand#42" "MEDIUM PLATED TIN"       45 4) ("Brand#42" "PROMO BRUSHED STEEL"     19 4)
    ("Brand#42" "PROMO BURNISHED TIN"     49 4) ("Brand#42" "PROMO PLATED STEEL"      19 4)
    ("Brand#42" "PROMO PLATED STEEL"      45 4) ("Brand#42" "STANDARD BURNISHED NICKEL" 49 4)
    ("Brand#42" "STANDARD PLATED COPPER"  19 4) ("Brand#43" "ECONOMY ANODIZED COPPER" 19 4)
    ("Brand#43" "ECONOMY ANODIZED NICKEL" 49 4) ("Brand#43" "ECONOMY PLATED TIN"      19 4)
    ("Brand#43" "ECONOMY POLISHED TIN"    45 4) ("Brand#43" "LARGE BURNISHED COPPER"   3 4)
    ("Brand#43" "LARGE POLISHED TIN"      45 4) ("Brand#43" "MEDIUM ANODIZED BRASS"   14 4)
    ("Brand#43" "MEDIUM ANODIZED COPPER"  36 4) ("Brand#43" "MEDIUM ANODIZED COPPER"  49 4)
    ("Brand#43" "MEDIUM BURNISHED TIN"    23 4) ("Brand#43" "PROMO BRUSHED BRASS"     36 4)
    ("Brand#43" "PROMO BURNISHED STEEL"    3 4) ("Brand#43" "PROMO POLISHED BRASS"    19 4)
    ("Brand#43" "SMALL BRUSHED NICKEL"     9 4) ("Brand#43" "SMALL POLISHED STEEL"    19 4)
    ("Brand#43" "STANDARD ANODIZED BRASS"  3 4) ("Brand#43" "STANDARD PLATED TIN"     14 4)
    ("Brand#44" "ECONOMY ANODIZED NICKEL" 36 4) ("Brand#44" "ECONOMY POLISHED NICKEL" 23 4)
    ("Brand#44" "LARGE ANODIZED BRASS"    19 4) ("Brand#44" "LARGE BRUSHED TIN"        3 4)
    ("Brand#44" "MEDIUM BRUSHED STEEL"    19 4) ("Brand#44" "MEDIUM BURNISHED COPPER" 45 4)
    ("Brand#44" "MEDIUM BURNISHED NICKEL" 23 4) ("Brand#44" "MEDIUM PLATED COPPER"    14 4)
    ("Brand#44" "SMALL ANODIZED COPPER"   23 4) ("Brand#44" "SMALL ANODIZED TIN"      45 4)
    ("Brand#44" "SMALL PLATED COPPER"     19 4) ("Brand#44" "STANDARD ANODIZED COPPER" 3 4)
    ("Brand#44" "STANDARD ANODIZED NICKEL" 36 4) ("Brand#51" "ECONOMY ANODIZED STEEL"  9 4)
    ("Brand#51" "ECONOMY PLATED NICKEL"   49 4) ("Brand#51" "ECONOMY POLISHED COPPER"  9 4)
    ("Brand#51" "ECONOMY POLISHED STEEL"  49 4) ("Brand#51" "LARGE BURNISHED BRASS"   19 4)
    ("Brand#51" "LARGE POLISHED STEEL"    19 4) ("Brand#51" "MEDIUM ANODIZED TIN"     14 4)
    ("Brand#51" "PROMO BRUSHED BRASS"     23 4) ("Brand#51" "PROMO POLISHED STEEL"    49 4)
    ("Brand#51" "SMALL BRUSHED TIN"       36 4) ("Brand#51" "SMALL POLISHED STEEL"    49 4)
    ("Brand#51" "STANDARD BRUSHED COPPER"  3 4) ("Brand#51" "STANDARD BRUSHED NICKEL" 19 4)
    ("Brand#51" "STANDARD BURNISHED COPPER" 19 4) ("Brand#52" "ECONOMY ANODIZED BRASS" 14 4)
    ("Brand#52" "ECONOMY ANODIZED COPPER" 36 4) ("Brand#52" "ECONOMY BURNISHED NICKEL" 19 4)
    ("Brand#52" "ECONOMY BURNISHED STEEL" 36 4) ("Brand#52" "ECONOMY PLATED TIN"      23 4)
    ("Brand#52" "LARGE BRUSHED NICKEL"    19 4) ("Brand#52" "LARGE BURNISHED TIN"     45 4)
    ("Brand#52" "LARGE PLATED STEEL"       9 4) ("Brand#52" "LARGE PLATED TIN"         9 4)
    ("Brand#52" "LARGE POLISHED NICKEL"   36 4) ("Brand#52" "MEDIUM BURNISHED TIN"    45 4)
    ("Brand#52" "SMALL ANODIZED NICKEL"   36 4) ("Brand#52" "SMALL ANODIZED STEEL"     9 4)
    ("Brand#52" "SMALL BRUSHED STEEL"     23 4) ("Brand#52" "SMALL BURNISHED NICKEL"  14 4)
    ("Brand#52" "STANDARD POLISHED STEEL" 19 4) ("Brand#53" "LARGE BURNISHED NICKEL"  23 4)
    ("Brand#53" "LARGE PLATED BRASS"       9 4) ("Brand#53" "LARGE PLATED STEEL"      49 4)
    ("Brand#53" "MEDIUM BRUSHED COPPER"    3 4) ("Brand#53" "MEDIUM BRUSHED STEEL"    45 4)
    ("Brand#53" "SMALL BRUSHED BRASS"     36 4) ("Brand#53" "STANDARD PLATED STEEL"   45 4)
    ("Brand#54" "ECONOMY ANODIZED BRASS"   9 4) ("Brand#54" "ECONOMY BRUSHED TIN"     19 4)
    ("Brand#54" "ECONOMY POLISHED BRASS"  49 4) ("Brand#54" "LARGE ANODIZED BRASS"    49 4)
    ("Brand#54" "LARGE BURNISHED BRASS"   49 4) ("Brand#54" "LARGE BURNISHED TIN"     14 4)
    ("Brand#54" "LARGE POLISHED BRASS"    19 4) ("Brand#54" "MEDIUM BURNISHED STEEL"   3 4)
    ("Brand#54" "SMALL BURNISHED STEEL"   19 4) ("Brand#54" "SMALL PLATED BRASS"      23 4)
    ("Brand#54" "SMALL PLATED TIN"        14 4) ("Brand#55" "LARGE BRUSHED NICKEL"     9 4)
    ("Brand#55" "LARGE PLATED TIN"         9 4) ("Brand#55" "LARGE POLISHED STEEL"    36 4)
    ("Brand#55" "MEDIUM BRUSHED TIN"      45 4) ("Brand#55" "PROMO BRUSHED STEEL"     36 4)
    ("Brand#55" "PROMO BURNISHED STEEL"   14 4) ("Brand#55" "SMALL PLATED COPPER"     45 4)
    ("Brand#55" "STANDARD ANODIZED BRASS" 36 4) ("Brand#55" "STANDARD BRUSHED COPPER"  3 4)
    ("Brand#55" "STANDARD BRUSHED STEEL"  19 4)))

(define tpch-q18-expected
  '(("Customer#000000667" 667 29158 9424 439687.23 305.0)
    ("Customer#000000178" 178  6882 9960 422359.65 303.0)))

(define tpch-q19-expected
  '((22923.028)))

(define tpch-q20-expected
  '(("Supplier#000000013" "HK71HQyWoqRWOX8GI FpgAifW,2PoH")))

(define tpch-q21-expected
  '(("Supplier#000000074" 9)))