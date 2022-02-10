FROM registry.gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark:1.5.16

USER root
ENTRYPOINT ["/usr/bin/entrypoint.sh"]