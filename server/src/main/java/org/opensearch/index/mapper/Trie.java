/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

public class Trie {

    private Node root = new Node();
    public Trie() {
    }

    public void put(String key, byte val) {
        Node current = root;
        for (int i = 0; i < key.length(); i ++) {
            current = current.children.computeIfAbsent((byte) key.charAt(i), kx -> new Node());
        }
        current.val = val;
        current.len = key.length();
    }

    private static class Node {
        byte key;
        private HashMap<Byte, Node> children = new HashMap<>();

        byte val = -1;
        int len = -1;
    }

    public static class Matcher {

        Node root;
        Node current;

        Node parent = null;

        public Matcher(Trie trie) {
            root = current = trie.root;
        }

        public Byte match(byte b) {
            Node next = current.children.get(b);
            if (next == null) {
                current = null;
                return null;
            }
            parent = current;
            current = next;
            return next.val;
        }

        public void reset() {
            current = root;
        }

        public int currentLength() {
            return current.len;
        }

        public boolean isFirstCharacter() {
            return parent == root;
        }
    }
}
