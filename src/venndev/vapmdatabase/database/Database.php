<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database;

abstract class Database
{

    abstract public function getDatabase(): mixed;

    abstract public function reconnect(): void;

    abstract public function execute(string $query, array $namedArgs = []): mixed;

    abstract public function executeSync(string $query, array $namedArgs = []): mixed;

}