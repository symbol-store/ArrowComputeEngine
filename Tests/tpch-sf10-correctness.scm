(import (scheme base)
        (scheme load)
        (chibi time)
        (chibi test)
        (BOSS))

(boss-eval (SetDefaultEnginePipeline "Build/libArrowComputeEngine.so"))

(load "Tests/tpch-queries.scm")

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

(test-group "TPC-H SF10 correctness"

  (test-assert "Q1"
    (check-expected
      (tpch-q1 (ByName lineitem) ,(date->days "1998-09-02"))
      tpch-q1-expected))

  (test-assert "Q2"
    (check-expected
      (tpch-q2 (ByName supplier) (ByName nation) (ByName region)
               (ByName partsupp) (ByName part))
      tpch-q2-expected))

  (test-assert "Q3"
    (check-expected
      (tpch-q3 (ByName customer) (ByName orders) (ByName lineitem)
               ,(date->days "1995-03-15"))
      tpch-q3-expected))

  (test-assert "Q4"
    (check-expected
      (tpch-q4 (ByName lineitem) (ByName orders)
               ,(date->days "1993-07-01") ,(date->days "1993-10-01"))
      tpch-q4-expected))

  (test-assert "Q5"
    (check-expected
      (tpch-q5 (ByName supplier) (ByName nation) (ByName region)
               (ByName customer) (ByName orders) (ByName lineitem)
               ,(date->days "1994-01-01") ,(date->days "1995-01-01"))
      tpch-q5-expected))

  (test-assert "Q6"
    (check-expected
      (tpch-q6 (ByName lineitem)
               ,(date->days "1994-01-01") ,(date->days "1995-01-01"))
      tpch-q6-expected))

  (test-assert "Q7"
    (check-expected
      (tpch-q7 (ByName nation) (ByName supplier) (ByName orders) (ByName customer)
               (ByName lineitem)
               ,(date->days "1995-01-01") ,(date->days "1996-12-31"))
      tpch-q7-expected))

  (test-assert "Q8"
    (check-expected
      (tpch-q8 (ByName nation) (ByName region) (ByName customer) (ByName supplier)
               (ByName part) (ByName orders) (ByName lineitem)
               ,(date->days "1995-01-01") ,(date->days "1996-12-31"))
      tpch-q8-expected))

  (test-assert "Q9"
    (check-expected
      (tpch-q9 (ByName nation) (ByName supplier) (ByName part)
               (ByName lineitem) (ByName partsupp) (ByName orders))
      tpch-q9-expected))

  (test-assert "Q10"
    (check-expected
      (tpch-q10 (ByName orders) (ByName customer) (ByName lineitem) (ByName nation)
                ,(date->days "1993-10-01") ,(date->days "1994-01-01"))
      tpch-q10-expected))

  (test-assert "Q11"
    (check-expected
      (tpch-q11 (ByName partsupp) (ByName supplier) (ByName nation) 10000000.0)
      tpch-q11-expected))

  (test-assert "Q12"
    (check-expected
      (tpch-q12 (ByName orders) (ByName lineitem)
                ,(date->days "1994-01-01") ,(date->days "1995-01-01"))
      tpch-q12-expected))

  (test-assert "Q13"
    (check-expected
      (tpch-q13 (ByName customer) (ByName orders))
      tpch-q13-expected))

  (test-assert "Q14"
    (check-expected
      (tpch-q14 (ByName lineitem) (ByName part)
                ,(date->days "1995-09-01") ,(date->days "1995-10-01"))
      tpch-q14-expected))

  (test-assert "Q16"
    (check-expected
      (tpch-q16 (ByName part) (ByName partsupp) (ByName supplier))
      tpch-q16-expected))

  (test-assert "Q18"
    (check-expected
      (tpch-q18 (ByName lineitem) (ByName customer) (ByName orders))
      tpch-q18-expected))

  (test-assert "Q19"
    (check-expected
      (tpch-q19 (ByName lineitem) (ByName part))
      tpch-q19-expected))

  (test-assert "Q20"
    (check-expected
      (tpch-q20 (ByName part) (ByName lineitem) (ByName partsupp) (ByName nation)
                (ByName supplier)
                ,(date->days "1994-01-01") ,(date->days "1995-01-01"))
      tpch-q20-expected))

  (test-assert "Q21"
    (check-expected
      (tpch-q21 (ByName lineitem) (ByName orders) (ByName supplier) (ByName nation))
      tpch-q21-expected)))
