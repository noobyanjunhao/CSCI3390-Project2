# Large Scale Data Processing: Project 2

## Team Members
- Junhao Yan, Bo Znang, Ruohang Feng

---

## Introduction

This project implements and analyzes various sketching algorithms for estimating frequency moments in large datasets using Apache Spark. The algorithms implemented include:

- Exact F2 calculation
- Tug-of-War sketching for F2 approximation
- BJKST sketching for F0 approximation

---

## 1. Exact F2 Implementation

The `exact_F2` function calculates the **exact second frequency moment (F2)** of a dataset, which is the sum of the squares of the frequencies of each element.

### Code:
```scala
def exact_F2(x: RDD[String]): Long = {
  x.map(s => (s, 1L))
   .reduceByKey(_ + _)
   .map { case (_, count) => count * count }
   .sum()
   .toLong
}
```

- Exact F2. Time elapsed: 116s. Estimate: 8567966130

## 2. Tug_of_War

Using 4-universal hash functions to create sketches.

### Code:
```scala
def Tug_of_War(x: RDD[String], width: Int, depth: Int): Long = {
  val hashFunctions = Array.fill(depth)(
    Array.fill(width)(new four_universal_Radamacher_hash_function())
  )

  val estimates = for (d <- 0 until depth) yield {
    val rowEstimates = for (w <- 0 until width) yield {
      val hashFunc = hashFunctions(d)(w)
      val sketch = x.map(s => hashFunc.hash(s)).reduce(_ + _)
      sketch * sketch
    }
    rowEstimates.sum / width
  }

  val sortedEstimates = estimates.sorted
  if (depth % 2 == 1)
    sortedEstimates(depth / 2)
  else
    (sortedEstimates(depth / 2 - 1) + sortedEstimates(depth / 2)) / 2
}
```

- Tug of War(10, 3). Time elapsed: 1129s Estimate: 9600622926
- Tug of War(1, 1) (faster than exact $F_2$). Time elapsed: 39s Estimate: 1180747044









