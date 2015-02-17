/*******************************************************************************
 *   Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package com.necla.simba.util;

import java.io.Serializable;

/***
 * Utility class to provide the pair data abstraction.
 */
public class Pair<A, B> implements Serializable {
	private static final long serialVersionUID = -5882474332890446940L;
	private A first;
	private B second;

	public Pair(A first, B second) {
		super();
		this.first = first;
		this.second = second;
	}

	public int hashCode() {
		int hashFirst = first != null ? first.hashCode() : 0;
		int hashSecond = second != null ? second.hashCode() : 0;

		return (hashFirst + hashSecond) * hashSecond + hashFirst;
	}

	@SuppressWarnings("unchecked")
	public boolean equals(Object other) {
		if (other instanceof Pair) {
			Pair<A, B> otherPair = (Pair<A, B>) other;
			return ((this.first == otherPair.first || (this.first != null && otherPair.first != null && this.first.equals(otherPair.first))) && (this.second == otherPair.second || (this.second != null
					&& otherPair.second != null && this.second.equals(otherPair.second))));
		}

		return false;
	}

	public A getFirst() {
		return first;
	}

	public void setFirst(A first) {
		this.first = first;
	}

	public B getSecond() {
		return second;
	}

	public void setSecond(B second) {
		this.second = second;
	}

	public String toString() {
		return "(" + first + ", " + second + ")";
	}

}
