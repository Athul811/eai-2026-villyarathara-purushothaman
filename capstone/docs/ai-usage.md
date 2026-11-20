# AI usage

## Tools used

Claude (Anthropic), in the claude.ai chat, for guidance during the capstone.

## What for

Planning the order of work from the brief and grading rules, the first version of the shared broker helper and the Dockerfiles for step 0, the structure of the runbook, and a first draft of agent-readiness.md. <add anything else you really used it for>

## What I changed or rejected

I replaced the step 0 skeleton with my own JavaScript services (server.js, translate.js, hops.js), so the generated src/ stubs were dead code and I deleted them. <add what you rewrote in the runbook and agent-readiness.md>

## What I verified, and how

I ran node tests/stage.mjs for stages 0 to 5 (28 passed) on a rebuilt stack, checked docker compose ps for healthy containers, and read the generated text against my own code before keeping it. <add specific checks, e.g. the 409 on a non-stuck saga>

## Dependencies beyond the allow-list

<none, or list each package outside the README's allow-list, and why>
