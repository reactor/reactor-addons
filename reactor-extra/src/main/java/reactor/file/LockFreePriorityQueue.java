/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (c) 2008 IBM Corporation
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

package reactor.file;

import java.util.ArrayList;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicMarkableReference;

/**
 * It is a thread-safe and lock-free queue. This lock free priority queue
 * implementation, which based on the algorithm defined in the follwoing paper:
 * Fast and Lock-Free Concurrent Priority Queues for Multi-Thread Systems By
 * Hakan Sundell and Philippas Tsigas
 *
 * @author raja
 *
 * @param <E>
 *            type of element in the priority queue
 *
 */

public class LockFreePriorityQueue<E> extends AbstractQueue<E> {

	/**
	 * Internal Priority Queue node class.
	 *
	 * @param <E>
	 *            type of element in node
	 */
	private static class Node<E> {
		// FIXME why use default access level here
		int level, validLevel;
		AtomicMarkableReference<E> data;
		Node<E> prev;
		ArrayList<AtomicMarkableReference<Node<E>>> next;
		E key; // FIXME key is not used

		/**
		 * default constructor.
		 */
		public Node() {
			this.data = new AtomicMarkableReference<E>(null, false);
			this.prev = null;
			this.next = new ArrayList<AtomicMarkableReference<Node<E>>>(
					MAXLEVEL);
			this.key = null;
			this.validLevel = -1;
			// FIXME level not initialized here
		}

		/**
		 * @param l
		 *            random level
		 * @param d
		 *            key
		 */
		public Node(int l, E d) {
			this.data = new AtomicMarkableReference<E>(d, false);
			this.next = new ArrayList<AtomicMarkableReference<Node<E>>>(
					MAXLEVEL);
			this.level = l;
			this.key = d;

		}

		/**
		 * @param l
		 *            random level
		 * @param d
		 *            key
		 * @param al
		 *            next node
		 */
		public Node(int l, E d, ArrayList<AtomicMarkableReference<Node<E>>> al) {
			this.level = l;
			this.key = d;
			this.data = new AtomicMarkableReference<E>(d, false);
			this.next = al;
		}

		/**
		 * @param p
		 *            prev pointer
		 */
		public void setPrev(Node<E> p) {
			this.prev = p;
		}

	}

	/**
	 * Pair of internal node.
	 *
	 * @param <E>
	 *            type of element in node
	 */
	private static class NodePair<E> {
		Node<E> n1; // FIXME why use default access level here
		Node<E> n2;

		/**
		 * @param nn1
		 *            the first node
		 * @param nn2
		 *            the second node
		 */
		public NodePair(Node<E> nn1, Node<E> nn2) {
			n1 = nn1;
			n2 = nn2;
		}
	}

	/**
	 * Iterator definition of priority queue.
	 */
	private class PQueueIterator implements Iterator<E> {

		// FIXME why use default access level here
		Node<E> cursor = head.next.get(0).getReference();

		/**
		 * {@inheritDoc}
		 */
		public boolean hasNext() {
			return cursor != tail;
		}

		/**
		 * {@inheritDoc}
		 */
		public E next() {
			if (cursor == tail)
				throw new NoSuchElementException();

			E result = cursor.data.getReference();
			cursor = cursor.next.get(0).getReference();
			return result;
		}

		/**
		 * {@inheritDoc}
		 */
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Max level.
	 */
	static final int MAXLEVEL = 10; // FIXME why use default access level here
	/**
	 *
	 */
	static final double SLCONST = 0.50; // FIXME why use default access level
	// here
	/**
	 * Head pointer.
	 */
	Node<E> head = new Node<E>(); // FIXME why use default access level here
	//REVIEW what is validLevel of head?
	/**
	 * Tail pointer.
	 */
	Node<E> tail = new Node<E>(); // FIXME why use default access level here
	//REVIEW what is validLevel of tail?

	/**
	 * Random number generator.
	 */
	// FIXME why use default access level here
	static final Random RAND_GEN = new Random();

	private final Comparator<? super E> comparator;

	/**
	 * default constructor.
	 */
	public LockFreePriorityQueue() {
		for (int i = 0; i < MAXLEVEL; i++) {
			head.next.add(new AtomicMarkableReference<Node<E>>(tail, false));
		}

		comparator = null;
	}

	/**
	 * @param cmp
	 *            customized comparator
	 */
	public LockFreePriorityQueue(Comparator<? super E> cmp) {
		for (int i = 0; i < MAXLEVEL; i++) {
			head.next.add(new AtomicMarkableReference<Node<E>>(tail, false));
		}

		comparator = cmp;
	}

	/**
	 * {@inheritDoc}
	 */
	public int size() {
		int size = 0;
		Iterator<E> itr = iterator();
		while (itr.hasNext()) {
			itr.next();
			size++;
		}
		return size;
	}

	/**
	 * {@inheritDoc}
	 */
	public E peek() {
		return head.next.get(0).getReference().data.getReference();
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean offer(E e) {
		if (e == null)
			throw new NullPointerException();
		return add(e);
	}

	/**
	 * @return first element
	 */
	public E peekFirst() {
		return head.next.get(0).getReference().data.getReference();
	}

	/**
	 * {@inheritDoc}
	 */
	public E poll() {
		return deleteMin();
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean contains(Object o) {
		E element;
		Iterator<E> itr = new PQueueIterator();
		while (itr.hasNext()) {
			element = itr.next();
			if (element.equals(o)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isEmpty() {
		return head.next.get(0).getReference() == tail;
		// if (head.next.get(0).getReference() == tail) {
		// return true;
		// } else {
		// return false;
		// }
	}

	/**
	 * {@inheritDoc}
	 */
	public Iterator<E> iterator() {
		return new PQueueIterator();
	}

	/**
	 * @param k1
	 *            the first element
	 * @param k2
	 *            the second element
	 * @return -1 if k1 is less than k2; 0 if k1 equals k2; otherwise 1
	 */
	private int compare(E k1, E k2) {

		if ((k1 == null) && (k2 == null))
			return 0;
		if (k1 == null)
			return -1;
		else if (k2 == null)
			return 1;
		else {
			if (comparator == null)
				return ((Comparable<? super E>) k1).compareTo(k2);
			else
				return comparator.compare(k1, k2);
		}

	}

	private Node<E> readnode(Node<E> n, int ll) {

		// if (n == tail) return tail;
		if (n.next.get(ll).isMarked())
			return null;
		else
			return n.next.get(ll).getReference();
	}

	private NodePair<E> readNext(Node<E> n1, int ll) {

		Node<E> n2, nn2;

		if (n1.data.isMarked()) {
			nn2 = helpDelete(n1, ll);
			n1 = nn2;
		}

		n2 = readnode(n1, ll);
		while (n2 == null) {
			nn2 = helpDelete(n1, ll);
			n1 = nn2;
			n2 = readnode(n1, ll);
		}

		return new NodePair<E>(n1, n2);
	}

	private NodePair<E> scanKey(Node<E> n1, int ll, Node<E> nk) {

		Node<E> n2;
		NodePair<E> tn1, tn2;

		tn1 = readNext(n1, ll);
		n1 = tn1.n1;
		n2 = tn1.n2;

		// while ((n2 != tail) &&
		// (compare(n2.data.getReference(),nk.data.getReference()) < 0)) {
		while ((compare(n2.data.getReference(), nk.data.getReference()) < 0)
				&& (n2 != tail)) {
			n1 = n2;
			tn2 = readNext(n1, ll);
			n1 = tn2.n1;
			n2 = tn2.n2;
		}

		return new NodePair<E>(n1, n2);
	}

	private Node<E> helpDelete(Node<E> n, int ll) {

		int i = 0;
		Node<E> prev, last, n2;
		NodePair<E> tn1, tn2;
		AtomicMarkableReference<Node<E>> tempn1;

		// Mark all the next pointers of the node to be deleted
		for (i = ll; i <= n.validLevel - 1; i++) {
			do {
				tempn1 = n.next.get(i);
				n2 = tempn1.getReference();
			} while (!tempn1.compareAndSet(n2, n2, false, true)
					&& !tempn1.isMarked());
		}

		// Get the previous pointer
		prev = n.prev;
		if ((prev == null) || (ll >= prev.validLevel)) {
			prev = head;
			for (i = MAXLEVEL - 1; i >= ll; i--) {
				tn1 = scanKey(prev, i, n);
				n2 = tn1.n2;
				prev = tn1.n1;
			}
		}

		Node<E> tmpN = n.next.get(ll).getReference();

		while (true) {
			if (n.next.get(ll).getReference() == null)
				break;
			tn2 = scanKey(prev, ll, n);
			last = tn2.n2;
			prev = tn2.n1;
			if (((last != n) || (n.next.get(ll).getReference() == null)))
				break;

			if (!tmpN.data.isMarked()) {
				if (prev.next.get(ll).compareAndSet(n, tmpN, false, false)) {
					n.next.get(ll).set(null, true);
					break;
				}
			} else
				tmpN = tmpN.next.get(ll).getReference();

			if (n.next.get(ll).getReference() == null)
				break;
		}

		return prev;
	}

	private int randomLevel() {
		//FIXME randomLeve is not suitable for priority queue
		int v = 1;

		while ((RAND_GEN.nextDouble() < SLCONST) && (v < MAXLEVEL - 1)) {
			v = v + 1;
		}

		return v;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean add(E d) {
		int level, i;

		Node<E> newN;
		Node<E> n1, n2;
		ArrayList<Node<E>> savedNode = new ArrayList<Node<E>>();
		NodePair<E> tn1, tn2, tn3;

		// FIXME no need to add MAXLEVEL nodes, only randomLevel is enough
		for (int ii = 0; ii < MAXLEVEL; ii++) {
			savedNode.add(new Node<E>());
		}

		level = randomLevel();
		newN = new Node<E>(level, d);

		n1 = head;

		for (i = MAXLEVEL - 1; i >= 1; i--) {
			tn1 = scanKey(n1, i, newN);
			n2 = tn1.n2;
			n1 = tn1.n1;
			if (i < level) {
				savedNode.set(i, n1);
			}
		}

		int kk = 0;
		while (true) {
			tn2 = scanKey(n1, 0, newN);
			n2 = tn2.n2;
			n1 = tn2.n1;

			if ((compare(d, n2.data.getReference()) == 0)
					&& (!n2.data.isMarked())) {
				if (n2.data.compareAndSet(n2.data.getReference(), d, false,
						false)) {
					return true;
				} else {
					continue;
				}
			}

			if (kk == 0) {
				newN.next.add(new AtomicMarkableReference<Node<E>>(n2, false));
				kk++;
				newN.validLevel = 0;
			} else {
				newN.next.set(0,
						new AtomicMarkableReference<Node<E>>(n2, false));
			}
			if (n1.next.get(0).compareAndSet(n2, newN, false, false)) {
				break;
			}
		}

		for (i = 1; i <= level - 1; i++) {
			newN.validLevel = i;
			n1 = savedNode.get(i);
			kk = 0;
			while (true) {

				tn3 = scanKey(n1, i, newN);
				n2 = tn3.n2;
				n1 = tn3.n1;

				if (kk == 0) {
					newN.next.add(new AtomicMarkableReference<Node<E>>(n2,
							false));
					kk++;
				} else {
					newN.next.set(i, new AtomicMarkableReference<Node<E>>(n2,
							false));
				}
				if (newN.data.isMarked())
					break;
				if (n1.next.get(i).compareAndSet(n2, newN, false, false)) {
					break;
				}
			}
		}

		newN.validLevel = level;

		if (newN.data.isMarked())
			newN = helpDelete(newN, 0);

		return true;
	}

	/**
	 * @return min element
	 */
	private E deleteMin() {
		Node<E> n2, last, prev;
		Node<E> n1 = null;
		E val;
		int i = 0;
		NodePair<E> tn1, tn2;
		AtomicMarkableReference<Node<E>> tempn1;
		int iflag = 0;

		prev = head;

		retry: while (true) {
			// REVIEW what is iflag used for?
			if (iflag != 1) {
				tn1 = readNext(prev, 0);
				n1 = tn1.n2;
				prev = tn1.n1;
				if (n1 == tail)
					return null;
			}
			iflag = 0;

			val = n1.data.getReference();
			if (!n1.data.isMarked()) {
				if (n1.data.compareAndSet(n1.data.getReference(), n1.data
						.getReference(), false, true)) {
					n1.prev = prev;
					break;
				} else {
					iflag = 1;
					continue retry;
				}
			} else if (n1.data.isMarked()) {
				n1 = helpDelete(n1, 0);
			}
			prev = n1;
		}
		// FIXME why is validLevel here? different from paper
		for (i = 0; i <= n1.validLevel - 1; i++) {
			do {
				tempn1 = n1.next.get(i);
				n2 = tempn1.getReference();
			} while (!tempn1.compareAndSet(n2, n2, false, true)
					&& !tempn1.isMarked()); // REVIEW different from paper
		}

		prev = head;

		// FIXME why is validLevel here? different from paper
		for (i = n1.validLevel - 1; i >= 0; i--) {
			Node<E> tmpN = n1.next.get(i).getReference();
			while (true) {
				if (n1.next.get(i).getReference() == null)
					break;
				tn2 = scanKey(prev, i, n1);
				last = tn2.n2;
				prev = tn2.n1;

				if (((last != n1) || (n1.next.get(i).getReference() == null)))
					break;

				if (!tmpN.data.isMarked()) {
					if (prev.next.get(i).compareAndSet(n1, tmpN, false, false)) {
						n1.next.get(i).set(null, true);
						break;
					}
				} else
					tmpN = tmpN.next.get(i).getReference();

				if (n1.next.get(i).getReference() == null)
					break;
			}

		}

		return val;
	}

}
