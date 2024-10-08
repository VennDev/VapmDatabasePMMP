<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database\mysql;

use Exception;
use mysqli;
use Throwable;
use venndev\vapmdatabase\database\Database;
use venndev\vapmdatabase\database\handler\CachingQueryHandler;
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
        if (count($namedArgs) > 0) {
            $query = QueryUtil::buildQueryByNamedArgs($query, $namedArgs);
        }

        return new Promise(function ($resolve, $reject) use ($query): void {
            $errors = [];
            $rejects = [];
            try {
                if (($cached = CachingQueryHandler::getResultFromCache($query)) !== null) {
                    $resolve($cached);
                }

                if ($this->mysqli === null) {
                    $this->reconnect();
                }

                while ($this->isBusy) {
                    FiberManager::wait();
                }

                $this->isBusy = true; // Set busy flag

                if (!$this->mysqli->query($query, MYSQLI_ASYNC)) {
                    throw new Exception($this->mysqli->error);
                }

                $poll = [$this->mysqli];
                $begin = microtime(true);
                $numQueries = 0;

                while (microtime(true) - $begin <= $this->queryTimeout) {
                    $numQueries = (int)mysqli_poll($poll, $errors, $rejects, $this->queryTimeout); // Convert to milliseconds
                    if ($numQueries > 0) break;
                    FiberManager::wait();
                }

                if ($numQueries === 0) {
                    throw new Exception("Query timeout!");
                }

                $result = $this->mysqli->reap_async_query();
                if ($result === false) {
                    throw new Exception($this->mysqli->error);
                }

                $resolve(new ResultQuery(
                    ResultQuery::SUCCESS,
                    '',
                    $errors,
                    $rejects,
                    is_bool($result) ? true : iterator_to_array($result->getIterator())
                ));
            } catch (Throwable $e) {
                $reject(new ResultQuery(ResultQuery::FAILED, $e->getMessage(), $errors, $rejects, null));
            } finally {
                $this->isBusy = false; // Reset busy flag
                $this->mysqli->next_result();
            }
        });
    }

    /**
     * @throws Throwable
     */
    public function executeSync(string $query, array $namedArgs = []): null|ResultQuery|Exception
    {
        if (count($namedArgs) > 0) $query = QueryUtil::buildQueryByNamedArgs($query, $namedArgs);
        $errors = [];
        $rejects = [];
        try {
            if (($cached = CachingQueryHandler::getResultFromCache($query)) !== null) return $cached;
            if ($this->mysqli === null) $this->reconnect();
            if (!$this->mysqli->query($query, MYSQLI_ASYNC)) throw new Exception($this->mysqli->error);

            $poll = [$this->mysqli];
            $begin = microtime(true);
            $numQueries = 0;

            while (microtime(true) - $begin <= $this->queryTimeout) {
                $numQueries = (int)mysqli_poll($poll, $errors, $rejects, $this->queryTimeout); // Convert to milliseconds
                if ($numQueries > 0) break;
            }

            if ($numQueries === 0) throw new Exception("Query timeout!");
            $result = $this->mysqli->reap_async_query();
            if ($result === false) throw new Exception($this->mysqli->error);

            return new ResultQuery(
                ResultQuery::SUCCESS,
                '',
                $errors,
                $rejects,
                is_bool($result) ? true : iterator_to_array($result->getIterator())
            );
        } catch (Throwable $e) {
            return new ResultQuery(ResultQuery::FAILED, $e->getMessage(), $errors, $rejects, null);
        } finally {
            $this->isBusy = false; // Reset busy flag
            $this->mysqli->next_result();
        }
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