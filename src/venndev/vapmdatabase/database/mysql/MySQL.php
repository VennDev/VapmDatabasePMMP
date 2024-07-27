<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database\mysql;

use mysqli;
use Throwable;
use venndev\vapmdatabase\database\Database;
use venndev\vapmdatabase\database\ResultQuery;
use venndev\vapmdatabase\utils\QueryUtil;
use vennv\vapm\FiberManager;
use vennv\vapm\Promise;
use function count;
use function mysqli_report;
use const MYSQLI_ASYNC;
use const MYSQLI_REPORT_ERROR;
use const MYSQLI_REPORT_STRICT;
use const MYSQLI_STORE_RESULT;

final class MySQL extends Database
{

    private ?mysqli $mysqli;

    private bool $isBusy = false;

    private int $queryTimeout = 2;

    public function __construct(
        private readonly string $host,
        private readonly string $username,
        private readonly string $password,
        private readonly string $databaseName,
        private readonly int    $port = 3306
    )
    {
        mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
        $this->mysqli = new mysqli($host, $username, $password, $databaseName, $port);
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getUsername(): string
    {
        return $this->username;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function getDatabaseName(): string
    {
        return $this->databaseName;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getDatabase(): ?mysqli
    {
        return $this->mysqli;
    }

    public function isBusy(): bool
    {
        return $this->isBusy;
    }

    public function getQueryTimeout(): int
    {
        return $this->queryTimeout;
    }

    public function setQueryTimeout(int $queryTimeout): void
    {
        $this->queryTimeout = $queryTimeout;
    }

    public function reconnect(): void
    {
        $this->mysqli = new mysqli($this->host, $this->username, $this->password, $this->databaseName, $this->port);
    }

    /**
     * @throws Throwable
     */
    public function execute(string $query, array $namedArgs = []): Promise
    {
        if (count($namedArgs) > 0) $query = QueryUtil::buildQueryByNamedArgs($query, $namedArgs);
        return new Promise(function ($resolve, $reject) use ($query): void {
            if ($this->mysqli === null) $this->reconnect();
            while ($this->isBusy) FiberManager::wait();

            $this->isBusy = true; // Set busy flag

            $this->mysqli->query($query, MYSQLI_STORE_RESULT | MYSQLI_ASYNC);

            $poll = [$this->mysqli];
            $errors = [];
            $rejects = [];
            $begin = microtime(true);
            $numQueries = 0;

//            while (microtime(true) - $begin <= $this->queryTimeout) {
//                $numQueries = (int)mysqli_poll($poll, $errors, $rejects, 0, $this->queryTimeout);
//                if ($numQueries > 0) break;
//                FiberManager::wait();
//            }

//            if ($numQueries === 0) {
//                $reject(new ResultQuery(ResultQuery::FAILED, "Query error!", $errors, $rejects, null));
//            } else {
//                $result = $this->mysqli->reap_async_query();
//                $result === false ? $reject(new ResultQuery(ResultQuery::FAILED, $this->mysqli->error, $errors, $rejects, null)) : $resolve(new ResultQuery(ResultQuery::SUCCESS, '', $errors, $rejects, is_bool($result) ? $result : iterator_to_array($result->getIterator())));
//            }

            try {
                $result = $this->mysqli->reap_async_query();
                $result === false ? $reject(new ResultQuery(ResultQuery::FAILED, $this->mysqli->error, $errors, $rejects, null)) : $resolve(new ResultQuery(ResultQuery::SUCCESS, '', $errors, $rejects, is_bool($result) ? $result : iterator_to_array($result->getIterator())));
            } catch (Throwable $e) {
                $reject(new ResultQuery(ResultQuery::FAILED, "Query error!", $errors, $rejects, null));
            }

            $this->mysqli->next_result();
            $this->mysqli->close();
            $this->mysqli = null;

            $this->isBusy = false; // Reset busy flag

            var_dump("AAA");
        });
    }

    public function close(): void
    {
        $this->mysqli->close();
    }

    public function __destruct()
    {
        $this->close();
    }

}