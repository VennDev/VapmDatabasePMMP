<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database;

use venndev\vapmdatabase\database\utils\Settings;

final class ResultQuery
{

    public const SUCCESS = 'success';

    public const FAILED = 'failed';

    private string $status;

    private string $reason;

    private array $errors;

    private array $rejects;

    private mixed $result;

    private float $timeResponse;

    public function __construct(string $status, string $reason, array $errors, array $rejects, mixed $result)
    {
        $this->status = $status;
        $this->reason = $reason;
        $this->errors = $errors;
        $this->rejects = $rejects;
        $this->result = $result;
        $this->timeResponse = microtime(true);
    }

    public function getStatus(): string
    {
        return $this->status;
    }

    public function getReason(): string
    {
        return $this->reason;
    }

    public function getErrors(): array
    {
        return $this->errors;
    }

    public function getRejects(): array
    {
        return $this->rejects;
    }

    public function getResult(): mixed
    {
        return $this->result;
    }

    public function getTimeResponse(): float
    {
        return $this->timeResponse;
    }

    public function getTimeRemaining(): float
    {
        return microtime(true) - $this->timeResponse;
    }

    public function isExpired(): bool
    {
        return $this->getTimeRemaining() > Settings::TIME_EXPIRE_CACHE;
    }

    public function toJson(): string
    {
        return json_encode([
            "status" => $this->status,
            "reason" => $this->reason,
            "errors" => $this->errors,
            "rejects" => $this->rejects,
            "result" => $this->result,
            "timeResponse" => $this->timeResponse
        ]);
    }

}