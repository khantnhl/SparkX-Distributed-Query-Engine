use sparkx::{QueryMemory, SparkXError};

#[test]
fn reservations_enforce_the_query_limit_and_release_on_drop() {
    let memory = QueryMemory::new(100);
    let reservation = memory.try_reserve(60).unwrap();

    assert_eq!(reservation.bytes(), 60);
    assert_eq!(memory.reserved_bytes(), 60);
    assert_eq!(memory.peak_bytes(), 60);

    let error = memory.try_reserve(41).unwrap_err();
    assert!(matches!(error, SparkXError::ResourceExhausted(_)));
    assert_eq!(memory.reserved_bytes(), 60);

    drop(reservation);
    assert_eq!(memory.reserved_bytes(), 0);
    assert_eq!(memory.peak_bytes(), 60);
}

#[test]
fn reservations_can_grow_and_shrink_without_losing_peak_usage() {
    let memory = QueryMemory::new(100);
    let mut reservation = memory.try_reserve(20).unwrap();

    reservation.try_grow(50).unwrap();
    assert_eq!(reservation.bytes(), 70);
    assert_eq!(memory.reserved_bytes(), 70);

    reservation.shrink(30);
    assert_eq!(reservation.bytes(), 40);
    assert_eq!(memory.reserved_bytes(), 40);
    assert_eq!(memory.peak_bytes(), 70);

    drop(reservation);
    assert_eq!(memory.reserved_bytes(), 0);
}
