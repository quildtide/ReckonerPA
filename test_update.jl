function test_update()
    center = 1500
    spread = 1500
    var = (1500/3)^2
    mu = 1500

    for i in 1:50
        var_old = var
        var = 1 / (1/var + 1/256)
        mu = (mu / var_old + (-1)^i * 16
        /256) * var 
        println("$(mu), $var")
    end
end