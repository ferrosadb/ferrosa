# ferrosa-plpgsql — roadmap

- Now: census subset (DO, trigger functions, scalar functions).
- Next: assignment casts through the host (`SELECT ($1)::type`) once the expression engine
  supports casts; `RAISE`; loops with a fuel bound.
- Later: `EXCEPTION` blocks (needs subtransactions).
