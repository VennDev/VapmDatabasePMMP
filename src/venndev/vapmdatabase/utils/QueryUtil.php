<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\utils;

final class QueryUtil
{

    public static function buildQueryByNamedArgs(string $query, array $namedArgs): string
    {
        $keys = array_keys($namedArgs);
        $values = array_values($namedArgs);
        $keys = array_map(function ($key) {
            return ':' . $key;
        }, $keys);
        return str_replace($keys, $values, $query);
    }

}