//package com.hftparser.verifiers;
//
//import java.util.Iterator;
//
///**
// * Created by patrick on 8/4/14.
// */
//class IteratorPair<T extends Iterable> {
//    Iterator<T> left;
//    Iterator<T> right;
//
//    IteratorPair(T left, T right) {
//        this.left = left.iterator();
//        this.right = right.iterator();
//    }
//
//    public Iterator<T> getLeft() {
//        return left;
//    }
//
//    public Iterator<T> getRight() {
//        return right;
//    }
//}
//
//class Pair<T> {
//    T left;
//    T right;
//
//    Pair(T left, T right) {
//        this.left = left;
//        this.right = right;
//    }
//
//    public T getLeft() {
//        return left;
//    }
//
//    public T getRight() {
//        return right;
//    }
//}
//
//public class ZippedIterable<T extends Iterable> implements Iterable<Pair<T>> {
//    IteratorPair<T> iterablePair;
//
//    class ZippedIterator implements Iterator<Pair<T>> {
//
//        @Override
//        public boolean hasNext() {
//            return iterablePair.getLeft().hasNext() && iterablePair.getRight().hasNext();
//        }
//
//        @Override
//        public Pair<T> next() {
//            return new Pair(iterablePair.getLeft().next(), iterablePair.getRight().next());
//        }
//
//        @Override
//        public void remove() {
//            throw new UnsupportedOperationException();
//        }
//    }
//
//    public ZippedIterable(IteratorPair<T> iterablePair) {
//        this.iterablePair = iterablePair;
//    }
//
//    public ZippedIterable(T left, T right) {
//        this(new IteratorPair<T>(left, right));
//    }
//
//    @Override
//    public Iterator<Pair<T>> iterator() {
//        return new ZippedIterator();
//    }
//}
//
