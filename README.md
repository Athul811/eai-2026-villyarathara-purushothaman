# Enterprise Application Integration — autumn 2026

Turība University · Mārtiņš Leitass · all sessions **Wednesday 09:00–12:15**

This repository is where every assignment, scaffold, public test and learning objective for the course is published. It is the only place they are published. Watch it, or check it before each session.

- **[SYLLABUS.md](SYLLABUS.md)** — the rules. The progression gate, the late penalty, how marks are composed. Read it once, properly, before the first deadline.
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — how to ask a question, what to do when you are stuck, and what I need from you to be able to help.
- **[s0/](s0/)** — start here. The Docker pre-flight, due **2026-09-14**.

---

## Start here, this week

```bash
git clone https://github.com/mleitass/eai-2026.git
cd eai-2026/s0
make pull          # DO THIS AT HOME, BEFORE SESSION 1  (~350 MB)
make up
make doctor        # aiming for: DOCTOR: PASS
```

On Windows you do not need `make` — [`s0/README.md`](s0/README.md) has the plain PowerShell equivalent of every command.

Three things to do before Session 1 on **2026-09-02**:

1. Run `make pull` at home. Fourteen people pulling images on campus wifi at 09:00 does not work.
2. Read [SYLLABUS.md](SYLLABUS.md). The progression gate is unusual and it is the part that catches people.
3. Create your semester repository — see below.

---

## Your repository

**One repository for the whole semester.** You create it once, in week one,
and everything you submit for the rest of the course goes into it.

| | |
|---|---|
| **Name** | `eai-2026-<surname>` — lowercase, no diacritics. `eai-2026-berzins`, not `eai-2026-Bērziņš` |
| **Visibility** | **Public.** A private repository cannot be graded, and the portal will reject it |
| **Host** | GitHub |
| **Structure** | one folder per assignment, exactly as named below |

```
eai-2026-<surname>/
  s0/          doctor-output.txt, doctor-screenshot.png
  pa1/ … pa7/  your implementation, plus docs/adr-NNN.md
  canonical/   copied from this repository at PA4, unchanged
  capstone/    the final integration (PA8)
  README.md    your name, and anything I need to know to run your code
```

Why one repository: your capstone is an integration of your own PA4–PA7 work,
not a fresh start. By November the code you need is already sitting in the
repository next to the code you are writing, and the git history is the
evidence that you wrote it.

### Starting an assignment

Run `git pull` in your clone of this repository, then copy the assignment's
**whole folder** into your repository, unchanged, and work there:

```bash
# from the folder that holds both repositories
cp -r eai-2026/pa4 eai-2026-<surname>/
cp -r eai-2026/canonical eai-2026-<surname>/    # once, at PA4 — PA5 onward use it too
```

PowerShell: `Copy-Item -Recurse eai-2026\pa4 eai-2026-<surname>\` (same for
`canonical`).

Copy the folder, not just `starter/`: the tests, fixtures,
`docker-compose.yml` and ADR template are part of the assignment, and the
tests find them by relative path. If a published assignment later gets a
`## Changes` entry, it names the files that changed — copy those again.

---

## The semester

| S | Date | Room | Session | Assignment | Due 20:00 | Hard cut-off |
|---|---|---|---|---|---|---|
| — | — | — | — | [**Session 0** — Docker pre-flight](s0/) | **09-14** | 09-21 |
| 1 | **09-02** | C113 | Integration landscape, coupling and styles | [PA1 — legacy file ingestion](pa1/) | **09-14** | 09-21 |
| 2 | **09-16** | A410 | Messaging fundamentals | [PA2 — RabbitMQ publish and consume](pa2/) | **09-28** | 10-05 |
| 3 | **09-23** | C111 | Routing patterns | [PA3 — splitter, router, aggregator](pa3/) *(Python)* | **10-05** | 10-12 |
| 4 | **09-30** | C211 | Transformation and the canonical model | [PA4 — three sources to canonical](pa4/) | **10-12** | 10-19 |
| 5 | **10-14** | A410 | Reliable messaging | [PA5 — events, DLQ, idempotency](pa5/) | **10-26** | 11-02 |
| 6 | **10-21** | C211 | APIs and contracts | [PA6 — contract-first API and saga](pa6/) | **11-02** | 11-09 |
| 7 | **10-28** | C113 | The complete business process | [PA7 — port the saga to Temporal](pa7/) | **11-09** | 11-16 |
| 8 | **11-11** | C113 | Operating what you built, capstone kickoff | [PA8 — the capstone](capstone/) | **11-25** | — |
| Exam | *TBD* | — | Capstone defence, ~20 min each, includes an incident drill | — | — | — |

All dates are 2026. All times are **Europe/Riga**.

Note the gap structure: 14-7-7-14-7-7-14 days between sessions. Every
assignment gets a 12-day window regardless.

### The session (195 min)

| | |
|---|---|
| 20 min | **Overview** — the session's learning outcomes, and the parts people actually get stuck on |
| 40 min | **Q&A — your questions.** Prepared before you arrive |
| 15 min | Break |
| 90 min | **Open lab** — you work on the assignment, I circulate |
| 30 min | Assignment briefing and wrap |

**You are expected to attend, and attendance is registered by scanning a QR
code in the session. Staying is your own decision** — scan, and if you judge
the morning is better spent elsewhere, leaving is your right. I would rather
that than a room of people present and asleep. See
[SYLLABUS.md](SYLLABUS.md) §11, which is provisional pending confirmation
against university regulation.

**No content depends on being in the room.** Everything you need to pass is in
this repository: the objectives seven days ahead, the decks as pre-reading, the
assignment brief, the public tests. That is deliberate, and it obliges me
rather than you. Attendance earns no marks and buys no extension.

What a session gives you that this repository cannot is 195 minutes of direct
access to someone who has built and operated these systems for a living — and
that is the only reason to stay once you have scanned. The 40-minute Q&A is the
centre of it, and it only works one way: **arrive with your questions written
down.** Each `objectives/sN.md` names the hard parts and gives you self-check
questions to find your own gaps — whatever you cannot answer is your question.

A Q&A where nobody has prepared anything is forty minutes of me talking, which
is precisely the thing you could have read instead.

The 90-minute lab is unstructured on purpose. Bring your laptop with the
environment working — that is what Session 0 is for.

---

## How an assignment works

**1. Objectives are published seven days before the session.**
[`objectives/sN.md`](objectives/) tells you what you will be able to do by the
end, which parts are genuinely hard, what to read beforehand, and self-check
questions to find your own gaps. The session will not require anything that is
not on that page — which is why attendance is optional and why the seven days
are there for you to arrive with questions instead of gaps.

**2. The assignment is published at the session**, in `paN/`. Each one ships:

```
README.md          the brief
starter/           scaffold with TODOs
tests/public/      run these locally, as often as you like
docs/adr-NNN.md    the ADR template — an ADR is required
docker-compose.yml the services the assignment needs (PA2 onward)
```

**3. You implement it in your own repository**, in the matching folder.

**4. You run the public tests locally** until they pass. They are the same
tests I run. There is no reason to submit without having run them.

**5. Hidden tests also run at grading time.** They are never published. They
test the same requirements as the public ones and exist so that code written
to pass the visible tests specifically does not score well.

**6. You submit through the portal** — **<https://evaluentis.leitass.eu>**,
live before the first deadline on 2026-09-14. Never by email. Email is not a
submission channel in this course, for anything.

### The mark

| | |
|---|---|
| **Each assignment** | 70% automated tests + 30% manual (ADR quality, self-assessment honesty) |
| **Homework total** | the average of your seven assignment marks, PA1–PA7 = **50%** |
| **Session 0** | pass/fail, unweighted — but still gated: it must be submitted |
| **Capstone** | **50%** |
| **To pass** | homework average ≥ 50% **and** capstone ≥ 50% |

### The two rules that eliminate people

**The progression gate.** Every assignment must be *submitted* before its hard
cut-off, which is its deadline plus seven days — that is eight submissions:
Session 0, then PA1 through PA7. Miss one and **your capstone is not graded**,
which fails the course no matter how good the rest of your work was. Session 0
is pass/fail and carries no weight, but it is gated like the rest.

**The late penalty.** −2% of that assignment's mark per **started** hour, from
the deadline. Twenty-five hours late is −50%, which fails perfect work.

Both are in [SYLLABUS.md](SYLLABUS.md) with worked examples. Neither has an
exception for laptop problems, Docker problems, or other courses' deadlines.

---

## The stack

| | |
|---|---|
| **Primary language** | **TypeScript** — PA1, PA2, PA4–PA8. Node 20 LTS, TypeScript 5.x, `tsx` |
| **Second language** | **Python 3.12** — the PA3 scaffold only |
| **Broker** | RabbitMQ |
| **Durable execution** | Temporal (PA7, and a bonus in the capstone) |
| **Everything runs in** | Docker Compose v2 |

**On Python.** PA3 is scaffolded in Python because reading and extending a
service in an unfamiliar language is itself an integration skill, and it is
deliberately the only point in the course where you have to. Elsewhere you may
implement in Python instead of TypeScript — the tests are black-box and will
grade your submission identically. No Python scaffold is provided outside PA3,
and Python-specific problems are not covered in office hours.

---

## What is in this repository

```
SYLLABUS.md        the rules: gate, penalties, marks, extensions
CONTRIBUTING.md    how to ask for help
objectives/        s1.md … s8.md — published 7 days before each session
s0/                the Docker pre-flight. Start here
pa1/ … pa8/        one folder per assignment
canonical/         order.schema.json — the fixed canonical model, from PA4 on
capstone/          the final integration: brief, rubric, integration tests
```

Folders for assignments that have not been published yet contain a placeholder
README with the final dates in it, so you can plan the semester now.

---

## Changes to this repository

Assignments are published, then left alone. If I have to correct a published
assignment I will note it in that assignment's README under a `## Changes`
heading with a date, and announce it in BATIS. Corrections never make
an assignment harder after publication.
