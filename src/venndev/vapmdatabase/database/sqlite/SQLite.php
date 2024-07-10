<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database\sqlite;

use Exception;
use SQLite3;
use Throwable;
use venndev\vapmdatabase\database\Database;
use venndev\vapmdatabase\database\ResultQuery;
use venndev\vapmdatabase\utils\QueryUtil;
use vennv\vapm\Promise;
use const SQLITE3_OPEN_CREATE;
use const SQLITE3_OPEN_READWRITE;

final class SQLite extends Database
{

    private SQLite3 $sqlite;

    public function __construct(
        private readonly string $databasePath
    )
    {
        $this->sqlite = new SQLite3($databasePath, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE);
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        $this->sqlite->close();
    }

    public function getDatabasePath(): string
    {
        return $this->databasePath;
    }

    public function getDatabase(): SQLite3
    {
        return $this->sqlite;
    }

    /**
     * @param string $query
     * @param array $namedArgs
     * @return Promise
     * @throws Throwable
     */
    public function execute(string $query, array $namedArgs = []): Promise
    {
        $query = QueryUtil::buildQueryByNamedArgs($query, $namedArgs);
        return new Promise(function (callable $resolve, callable $reject) use ($query) {
            $result = $this->sqlite->query($query);
            if ($result === false) {
                $reject(new Exception($this->sqlite->lastErrorMsg()));
            } else {
                $resolve(new ResultQuery(
                    status: ResultQuery::SUCCESS,
                    reason: '',
                    errors: [],
                    rejects: [],
                    result: $result
                ));
            }
        });
    }

}