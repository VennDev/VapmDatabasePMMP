<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database\handler;

/**
 * This is class QueryHandler.
 * Handle too many repetitions of query statements
 */
final class QueryHandler
{

    private static float $timeExpire = 1.5;

    private static array $queryHandler = [];

    public function addQuery(string $query): void
    {
        self::$queryHandler[$query] = microtime(true);
    }

    public function removeQuery(string $query): void
    {
        unset(self::$queryHandler[$query]);
    }

    public function setExpireTime(float $time): void
    {
        self::$timeExpire = $time;
    }

    public function getExpireTime(): float
    {
        return self::$timeExpire;
    }

    public function processQuery(string $query, callable $callable): mixed
    {
        if (isset(self::$queryHandler[$query]) && microtime(true) - self::$queryHandler[$query] > self::$timeExpire) {
            unset(self::$queryHandler[$query]);
        }

        if (!isset(self::$queryHandler[$query])) {
            self::$queryHandler[$query] = microtime(true);
            return $callable();
        }

        return false;
    }

}