<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database\handler;

use venndev\vapmdatabase\database\ResultQuery;

final class CachingQueryHandler
{

    private static array $cache = [];

    public static function get(string $query): ?ResultQuery
    {
        return self::$cache[$query] ?? null;
    }

    public static function set(string $query, ResultQuery $resultQuery): void
    {
        self::$cache[$query] = $resultQuery;
    }

    public static function clear(): void
    {
        self::$cache = [];
    }

    public static function clearQuery(string $query): void
    {
        unset(self::$cache[$query]);
    }

    public static function getResultFromCache(string $query): ?ResultQuery
    {
        $cached = self::get($query);
        if ($cached !== null && !$cached->isExpired()) {
            return self::get($query);
        } elseif ($cached !== null && $cached->isExpired()) {
            self::clearQuery($query);
        }
        return null;
    }

}