# Large Scale Data Processing: Project 2

## Team Members
- Junhao Yan, Bo Znang, Ruohang Feng

---

## Introduction

This project implements and analyzes various sketching algorithms for estimating frequency moments in large datasets, including

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

## 3. BJKST

### Code:
```scala
class BJKSTSketch(bucket_in: Set[(String, Int)] ,  z_in: Int, bucket_size_in: Int) extends Serializable {
/* A constructor that requies intialize the bucket and the z value. The bucket size is the bucket size of the sketch. */

    var bucket: Set[(String, Int)] = bucket_in
    var z: Int = z_in
  
    val BJKST_bucket_size = bucket_size_in;

    def this(s: String, z_of_s: Int, bucket_size_in: Int){
      /* A constructor that allows you pass in a single string, zeroes of the string, and the bucket size to initialize the sketch */
      this(Set((s, z_of_s )) , z_of_s, bucket_size_in)
    }

    def +(that: BJKSTSketch): BJKSTSketch = {
      // Take the maximum z value
      val new_z = math.max(this.z, that.z)
      
      // Combine buckets, keeping only elements with z >= new_z
      var combined_bucket = this.bucket.filter(_._2 >= new_z) ++ that.bucket.filter(_._2 >= new_z)
      
      // If the combined bucket is too large, increase z until it fits
      var updated_z = new_z
      while (combined_bucket.size > BJKST_bucket_size) {
        updated_z += 1
        combined_bucket = combined_bucket.filter(_._2 >= updated_z)
      }
      
      new BJKSTSketch(combined_bucket, updated_z, BJKST_bucket_size)
    }

    def add_string(s: String, z_of_s: Int): BJKSTSketch = {
      // If z_of_s is less than current z, ignore this string
      if (z_of_s < z) return this
      
      // Add the string to the bucket
      var new_bucket = bucket + ((s, z_of_s))
      var new_z = z
      
      // If bucket is too large, increase z and filter
      while (new_bucket.size > BJKST_bucket_size) {
        new_z += 1
        new_bucket = new_bucket.filter(_._2 >= new_z)
      }
      
      new BJKSTSketch(new_bucket, new_z, BJKST_bucket_size)
    }
  }

def BJKST(x: RDD[String], width: Int, trials: Int): Double = {
  val hashFunctions = Seq.fill(trials)(new hash_function(Long.MaxValue))

  val sketches = x.mapPartitions(partition => {
    val localSketches = Array.fill(trials)(null: BJKSTSketch)
    partition.foreach(s => {
      for (i <- 0 until trials) {
        val h = hashFunctions(i)
        val hashValue = h.hash(s)
        val zeroes = h.zeroes(hashValue)
        if (localSketches(i) == null) {
          localSketches(i) = new BJKSTSketch(s, zeroes, width)
        } else {
          localSketches(i) = localSketches(i).add_string(s, zeroes)
        }
      }
    })
    Iterator(localSketches)
  }).reduce((sketches1, sketches2) => {
    for (i <- 0 until trials) {
      if (sketches1(i) == null) {
        sketches1(i) = sketches2(i)
      } else if (sketches2(i) != null) {
        sketches1(i) = sketches1(i) + sketches2(i)
      }
    }
    sketches1
  })

  val estimates = for (i <- 0 until trials) yield {
    if (sketches(i) == null) 0.0
    else math.pow(2, sketches(i).z) * sketches(i).bucket.size
  }

  val sortedEstimates = estimates.sorted
  if (trials % 2 == 1) sortedEstimates(trials / 2)
  else (sortedEstimates(trials / 2 - 1) + sortedEstimates(trials / 2)) / 2
}
```

- BJKST (Bucket Size: 100, Trials: 5). Time elapsed: 50s. Estimate: 6684672.0





