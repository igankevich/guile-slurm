# Introduction

Guile-slurm is a [Guile](https://www.gnu.org/software/guile/) API for
[SLURM](https://slurm.schedmd.com/) batch job scheduler.

# Usage

Here is an example of "Hello world" batch job.
We submit the job and wait for the completion.
```scheme
(use-modules
  (slurm)
  (oop goops))
(define job
  (make <job>
    #:name "hello-world"
    #:script "#!/bin/sh\necho Hello world\n"))
(submit-jobs `(,job))
```

More complex example shows submission of a job with dependencies.
```scheme
(use-modules
  (slurm)
  (oop goops))
(define job
  (make <job>
    #:name "hello-world"
    #:script "#!/bin/sh\necho Hello world\n"
    #:dependencies
    `(,(after (make <job> #:name "child-1" #:script "#!/bin/sh\necho child-1\n")
              (make <job> #:name "child-2" #:script "#!/bin/sh\necho child-2\n")))))
(submit-jobs `(,job))
```

# Installation

Copy `slurm.scm` into the default directory for Guile modules on your system.
Usually this is `/usr/share/guile/site/<version>`.
