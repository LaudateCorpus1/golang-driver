package cassandra

import (
    "errors"
    "fmt"
)

type Consistency uint16

const (
    Any         Consistency = 0x00
    One         Consistency = 0x01
    Two         Consistency = 0x02
    Three       Consistency = 0x03
    Quorum      Consistency = 0x04
    All         Consistency = 0x05
    LocalQuorum Consistency = 0x06
    EachQuorum  Consistency = 0x07
    LocalOne    Consistency = 0x0A
)

func (c Consistency) String() string {
    switch c {
    case Any:
        return "ANY"
    case One:
        return "ONE"
    case Two:
        return "TWO"
    case Three:
        return "THREE"
    case Quorum:
        return "QUORUM"
    case All:
        return "ALL"
    case LocalQuorum:
        return "LOCAL_QUORUM"
    case EachQuorum:
        return "EACH_QUORUM"
    case LocalOne:
        return "LOCAL_ONE"
    default:
        return fmt.Sprintf("UNKNOWN_CONS_0x%x", uint16(c))
    }
}

type Query struct {
    stmt string
    values []interface{}
    session *Session
    cons Consistency
}

// Iter represents an iterator that can be used to iterate over all rows that
// were returned by a query. The iterator might send additional queries to the
// database during the iteration if paging was enabled.
type Iter struct {
    err error
    result *Result
}

func (s *Session) Query(stmt string, values ...interface{}) *Query {
    return &Query {stmt: stmt, values: values, session: s, cons: One}
}

func (q *Query) Consistency(c Consistency) *Query {
    q.cons = c
    return q
}

func (q *Query) executeQuery() *Iter {
    stmt := NewStatement(q.stmt, len(q.values))
    defer stmt.Finalize()
    err := stmt.Bind(q.values)
    if err != nil {
        return &Iter{err:err}
    }
    stmtfuture := q.session.Execute(stmt)
    stmtfuture.Wait()
    defer stmtfuture.Finalize()
    result := stmtfuture.Result()
    return &Iter{err: nil, result: result}
}

// Iter executes the query and returns an iterator capable of iterating
// over all results.
func (q *Query) Iter() *Iter {
    return q.executeQuery()
}

func (i *Iter) Scan(dest ...interface{}) bool {
    if !i.result.Next() {
        return false
    }
    err := i.result.Scan(dest...)
    if err != nil {
        i.err = err
        return false
    }
    return true
}

func (i *Iter) Close() error {
    if i.err != nil {
        return i.err
    }
    i.result.Finalize()
    return nil
}

var (
    ErrNotFound      = errors.New("not found")
    ErrUnavailable   = errors.New("unavailable")
    ErrUnsupported   = errors.New("feature not supported")
    ErrTooManyStmts  = errors.New("too many statements")
    ErrSessionClosed = errors.New("session has been closed")
    ErrNoConnections = errors.New("no connections available")
    ErrNoKeyspace    = errors.New("no keyspace provided")
    ErrNoMetadata    = errors.New("no metadata available")
)
