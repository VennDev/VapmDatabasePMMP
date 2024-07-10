<?php

declare(strict_types=1);

namespace venndev\vapmdatabase\database;

final class ResultQuery
{

    public const SUCCESS = 'success';

    public const FAILED = 'failed';

    private string $status;

    private string $reason;

    private array $errors;

    private array $rejects;

    private mixed $result;

    public function __construct(string $status, string $reason, array $errors, array $rejects, mixed $result)
    {
        $this->status = $status;
        $this->reason = $reason;
        $this->errors = $errors;
        $this->rejects = $rejects;
        $this->result = $result;
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

    public function toJson(): string
    {
        return json_encode([
            "status" => $this->status,
            "reason" => $this->reason,
            "errors" => $this->errors,
            "rejects" => $this->rejects,
            "result" => $this->result
        ]);
    }

}