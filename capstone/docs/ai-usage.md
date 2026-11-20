# AI usage

## Tools used

ChatGPT (OpenAI) and Claude (Anthropic). I used Claude in the claude.ai chat across several separate accounts and conversations, and ChatGPT in its own chat.

## What for

[FILL IN, one line per tool and task. For example: "ChatGPT: <what you asked it for>." "Claude, other conversations: <what: e.g. the intake, orchestrator, notifier and observer services in steps 1 to 5>."]
In the Claude conversation for this final stretch: the order of work from the brief and the grading rules, a first skeleton of four services with a shared broker helper and Dockerfiles for step 0, the heading structure of runbook.md, a first draft of agent-readiness.md, and first drafts of ADR-007 to ADR-010, written from code lines and test output that I pasted in.

## What I changed or rejected

I replaced the step 0 skeleton with my own JavaScript services (server.js, translate.js, hops.js), so the generated src/ stubs were dead code and I deleted them. In ADR-010 I added what I observed in my own kill test: after killing the orchestrator during a slow shipping call, the saga was needs_attention with payment and inventory done, and shipping still created a shipment about five seconds later. I removed three sentences from the ADR drafts that the AI had guessed and I had not checked. [ADD what you changed, fixed or rejected in the code from the other tools, if anything.]

## What I verified, and how

I ran node tests/stage.mjs for stages 0 to 5 (28 passed) on a rebuilt stack, and checked docker compose ps for healthy containers. I ran the orchestrator kill test and compared the ADR text with /sagas/stuck, the trace and the shipping mock's effects log. I checked the code lines cited in the ADRs against orchestrator/server.js, intake/hops.js and observer/server.js.

## Dependencies beyond the allow-list

[REPLACE after checking the four package.json files against the README's allow-list.]
