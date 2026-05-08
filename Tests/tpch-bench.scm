(import (scheme load)
        (chibi time)
 )

(boss-eval (SetDefaultEnginePipeline "Build/libArrowComputeEngine.so"))

;;; boss-run must be defined here (not in tpch-queries.scm) because macro
;;; expansions look up boss-run in the call-site environment.  Chibi's
;;; (import ...) creates a scope local to the loaded file, so definitions
;;; inside tpch-queries.scm after its own import are invisible here.
(define (boss-run expr)
  (let ((b (convert-to-boss-expression expr)))
    (boss-expression-transfer! b)
    (convert-from-boss-expression (BOSSEvaluate b))))

;;; Load shared query plans
(load "Tests/tpch-queries.scm")

;;; ---------------------------------------------------------------------------
;;; Load TPC-H tables
;;;
;;; Expects standard dbgen .tbl files (pipe-delimited, no header, trailing |)
;;; in the current working directory.  Each Load call names the result for
;;; reuse across queries.
;;; ---------------------------------------------------------------------------

(boss-eval (Name (Load "TPCHData/lineitem.tbl"
  l_orderkey l_partkey l_suppkey l_linenumber l_quantity l_extendedprice
  l_discount l_tax l_returnflag l_linestatus l_shipdate l_commitdate
  l_receiptdate l_shipinstruct l_shipmode l_comment) lineitem))

(boss-eval (Name (Load "TPCHData/orders.tbl"
  o_orderkey o_custkey o_orderstatus o_totalprice o_orderdate o_orderpriority
  o_clerk o_shippriority o_comment) orders))

(boss-eval (Name (Load "TPCHData/customer.tbl"
  c_custkey c_name c_address c_nationkey c_phone c_acctbal c_mktsegment
  c_comment) customer))

(boss-eval (Name (Load "TPCHData/supplier.tbl"
  s_suppkey s_name s_address s_nationkey s_phone s_acctbal s_comment) supplier))

(boss-eval (Name (Load "TPCHData/part.tbl"
  p_partkey p_name p_mfgr p_brand p_type p_size p_container p_retailprice
  p_comment) part))

(boss-eval (Name (Load "TPCHData/partsupp.tbl"
  ps_partkey ps_suppkey ps_availqty ps_supplycost ps_comment) partsupp))

(boss-eval (Name (Load "TPCHData/nation.tbl"
  n_nationkey n_name n_regionkey n_comment) nation))

(boss-eval (Name (Load "TPCHData/region.tbl"
  r_regionkey r_name r_comment) region))

;;; ---------------------------------------------------------------------------
;;; Run queries
;;; ---------------------------------------------------------------------------

(time "Q1"
  (tpch-q1 (ByName lineitem) (date->days "1998-09-02")))

(time "Q2"
  (tpch-q2 (ByName supplier) (ByName nation) (ByName region)
           (ByName partsupp) (ByName part)))

(time "Q3"
  (tpch-q3 (ByName customer) (ByName orders) (ByName lineitem) (date->days "1995-03-15")))

(time "Q4"
  (tpch-q4 (ByName lineitem) (ByName orders) (date->days "1993-07-01") (date->days "1993-10-01")))

(time "Q5"
  (tpch-q5 (ByName supplier) (ByName nation) (ByName region)
           (ByName customer) (ByName orders) (ByName lineitem)
           (date->days "1994-01-01") (date->days "1995-01-01")))

(time "Q6"
  (tpch-q6 (ByName lineitem) (date->days "1994-01-01") (date->days "1995-01-01")))

(time "Q7"
  (tpch-q7 (ByName nation) (ByName supplier) (ByName orders) (ByName customer)
           (ByName lineitem) (date->days "1995-01-01") (date->days "1996-12-31")))

(time "Q8"
  (tpch-q8 (ByName nation) (ByName region) (ByName customer) (ByName supplier)
           (ByName part) (ByName orders) (ByName lineitem)
           (date->days "1995-01-01") (date->days "1996-12-31")))

(time "Q9"
  (tpch-q9 (ByName nation) (ByName supplier) (ByName part)
           (ByName lineitem) (ByName partsupp) (ByName orders)))

(time "Q10"
  (tpch-q10 (ByName orders) (ByName customer) (ByName lineitem) (ByName nation)
            (date->days "1993-10-01") (date->days "1994-01-01")))

(time "Q11"
  (tpch-q11 (ByName partsupp) (ByName supplier) (ByName nation) 1000000.0))

(time "Q12"
  (tpch-q12 (ByName orders) (ByName lineitem) (date->days "1994-01-01") (date->days "1995-01-01")))

(time "Q13"
  (tpch-q13 (ByName customer) (ByName orders)))

(time "Q14"
  (tpch-q14 (ByName lineitem) (ByName part) (date->days "1995-09-01") (date->days "1995-10-01")))

(time "Q16"
  (tpch-q16 (ByName part) (ByName partsupp) (ByName supplier)))

(time "Q18"
  (tpch-q18 (ByName lineitem) (ByName customer) (ByName orders)))

(time "Q19"
  (tpch-q19 (ByName lineitem) (ByName part)))

(time "Q20"
  (tpch-q20 (ByName part) (ByName lineitem) (ByName partsupp) (ByName nation)
            (ByName supplier) (date->days "1994-01-01") (date->days "1995-01-01")))

(time "Q21"
  (tpch-q21 (ByName lineitem) (ByName orders) (ByName supplier) (ByName nation)))
