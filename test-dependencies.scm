(use-modules (slurm) (oop goops))
(define job
  (make <job>
    #:name "parent"
    #:script "#!/bin/sh\necho Hello world\n"
    #:dependencies
    `(,(after (make <job> #:name "child-1" #:script "#!/bin/sh\necho child-1\n"))
       ,(after (make <job> #:name "child-2" #:script "#!/bin/sh\necho child-2\n")
               (make <job> #:name "child-3" #:script "#!/bin/sh\necho child-3\n")))))
(submit-jobs `(,job) #:wait? #f)
