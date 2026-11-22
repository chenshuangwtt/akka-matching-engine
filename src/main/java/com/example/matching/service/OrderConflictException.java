package com.example.matching.service;

public class OrderConflictException extends RuntimeException {
    public OrderConflictException(String message) { super(message); }
}
