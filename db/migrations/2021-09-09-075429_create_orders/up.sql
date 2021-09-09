create table orders
(
    id         varchar primary key not null,
    exchange   varchar             not null,
    pair       varchar             not null,
    details    text                not null,
    status     varchar             not null,
    updated_at int                 not null,
    created_at int                 not null,
)
