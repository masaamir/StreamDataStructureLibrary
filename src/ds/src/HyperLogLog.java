/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ds.src;

import static ds.util.HyperLogLogUtil.computeHash;
import ds.util.BucketAndHash;
import ds.util.HyperLogLogUtil;

public class HyperLogLog {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	byte[] buckets;

	// The current sum of 1 / (1L << buckets[i]). Updated as new items are added
	// and used for
	// estimation
	private double currentSum;
	private int nonZeroBuckets = 0;

	public HyperLogLog(int numberOfBuckets) {

		buckets = new byte[numberOfBuckets];
		currentSum = buckets.length;
	}

	public HyperLogLog(int[] buckets) {
		this(buckets.length);

		currentSum = 0;
		nonZeroBuckets = 0;
		for (int i = 0; i < buckets.length; i++) {
			int value = buckets[i];
			if (value != 0) {
				nonZeroBuckets++;
			}
			this.buckets[i] = (byte) value;
			currentSum += 1.0 / (1 << value);
		}
	}

	public void add(long value) {
		BucketAndHash bucketAndHash = BucketAndHash.fromHash(
				computeHash(value), buckets.length);
		int bucket = bucketAndHash.getBucket();

		int lowestBitPosition = Long.numberOfTrailingZeros(bucketAndHash
				.getHash()) + 1;
		// int lowestBitPosition = 2;

		int previous = buckets[bucket];

		if (previous == 0) {
			nonZeroBuckets++;
		}

		if (lowestBitPosition > previous) {
			currentSum -= 1.0 / (1L << previous);
			currentSum += 1.0 / (1L << lowestBitPosition);

			buckets[bucket] = (byte) lowestBitPosition;
		}
	}

	public long estimate() {
		double alpha = HyperLogLogUtil.computeAlpha(buckets.length);
		double result = alpha * buckets.length * buckets.length / currentSum;

		if (result <= 2.5 * buckets.length) {
			// adjust for small cardinalities
			int zeroBuckets = buckets.length - nonZeroBuckets;
			if (zeroBuckets > 0) {
				result = buckets.length
						* Math.log(buckets.length * 1.0 / zeroBuckets);
			}
		}

		return Math.round(result);
	}

	// public long estimateNew() {
	// double alpha = HyperLogLogUtil.computeAlpha(buckets.length);
	// double result = alpha * buckets.length * buckets.length /
	// getCurrentSum();
	//
	// if (result <= 2.5 * buckets.length) {
	// // adjust for small cardinalities
	// int zeroBuckets = buckets.length - nonZeroBuckets;
	// if (zeroBuckets > 0) {
	// result = buckets.length
	// * Math.log(buckets.length * 1.0 / zeroBuckets);
	// }
	// }
	//
	// return Math.round(result);
	// }
	//
	// public double getCurrentSum() {
	// double sum = buckets.length;
	// nonZeroBuckets = 0;
	// for (int i = 0; i < this.buckets.length; i++) {
	// if (buckets[i] != 0) {
	// sum += 1.0 / (1L << buckets[i]);
	// nonZeroBuckets++;
	// }
	//
	// }
	// return sum;
	// }

	public int[] buckets() {
		int[] result = new int[buckets.length];

		for (int i = 0; i < buckets.length; i++) {
			result[i] = buckets[i];
		}

		return result;
	}

	public void union(int[] newBuckets) {
		for (int i = 0; i < this.buckets.length; i++) {
			int previous = buckets[i];
			int lowestBitPosition = newBuckets[i];
			if (lowestBitPosition != 0) {
				if (previous == 0) {
					nonZeroBuckets++;
				}

				if (lowestBitPosition > previous) {
					currentSum -= 1.0 / (1L << previous);
					currentSum += 1.0 / (1L << lowestBitPosition);

					buckets[i] = (byte) lowestBitPosition;
				}
			}
		}
	}

}
