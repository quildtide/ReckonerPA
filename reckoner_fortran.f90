elemental function win_chance_to_rank(win_chance) result(rank)
    implicit none
    real(kind=8), intent(in) :: win_chance
    real(kind=8) :: rank, base_rank, threshold

    base_rank = 1000.0D0

    rank = base_rank / (1 - win_chance) - base_rank

    threshold = 2000.0D0

    if (rank > threshold) then
        rank = log(rank) / log(threshold) * threshold
    end if

end function win_chance_to_rank